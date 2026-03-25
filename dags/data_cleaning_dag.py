"""
DAG: data_cleaning
Daily automated data cleaning pipeline.

Flow:
  1. Setup Airflow connections (S3 + Yandex Cloud SA)
  2. Detect new (unprocessed) files in S3 bucket
  3. Create ephemeral DataProc Spark cluster
  4. Run data_cleaning.py via spark-submit on all files
  5. Delete DataProc cluster
"""

import uuid
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# ============================================================
# Variables (loaded from Airflow UI -> Admin -> Variables)
# ============================================================

YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

DP_SA_ID = Variable.get("DP_SA_ID")
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

# ============================================================
# Connections
# ============================================================

YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)

YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)


def setup_airflow_connections(**kwargs):
    """Create connections if they don't exist."""
    session = Session()
    try:
        for conn in [YC_S3_CONNECTION, YC_SA_CONNECTION]:
            if not session.query(Connection).filter(
                Connection.conn_id == conn.conn_id
            ).first():
                session.add(conn)
                print(f"Added connection: {conn.conn_id}")
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def detect_new_files(**kwargs):
    """
    Find .txt files in bucket root that don't have corresponding
    cleaned parquet output yet. Push the list to XCom.
    Returns True if there are new files, False otherwise.
    """
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

    # List all .txt files in bucket root
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="", Delimiter="/")
    all_txt = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".txt")
    ]

    # List already cleaned parquet partitions
    cleaned_response = s3.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="cleaned/", Delimiter="/"
    )
    has_cleaned = bool(cleaned_response.get("CommonPrefixes") or
                       cleaned_response.get("Contents"))

    # If cleaned/ exists, find files that were added after the last cleaning
    # For simplicity: track processed files via a manifest
    manifest_key = "cleaned/_processed_files.json"
    processed = set()
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=manifest_key)
        processed = set(json.loads(obj["Body"].read().decode()))
    except s3.exceptions.NoSuchKey:
        pass
    except Exception:
        pass

    new_files = [f for f in all_txt if f not in processed]
    print(f"Total .txt files: {len(all_txt)}")
    print(f"Already processed: {len(processed)}")
    print(f"New files to process: {len(new_files)}")

    if new_files:
        kwargs["ti"].xcom_push(key="new_files", value=",".join(new_files))
        kwargs["ti"].xcom_push(key="all_processed",
                               value=json.dumps(list(processed | set(new_files))))
    return len(new_files) > 0


def update_manifest(**kwargs):
    """Update the processed files manifest after successful cleaning."""
    import boto3

    all_processed = kwargs["ti"].xcom_pull(
        task_ids="detect_new_files", key="all_processed"
    )
    if not all_processed:
        return

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key="cleaned/_processed_files.json",
        Body=all_processed.encode(),
    )
    print(f"Updated manifest with {len(json.loads(all_processed))} files")


# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="data_cleaning",
    start_date=datetime(2026, 3, 15),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    # Step 0: setup connections
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=setup_airflow_connections,
    )

    # Step 1: detect new files (skip DAG if nothing new)
    detect_new = ShortCircuitOperator(
        task_id="detect_new_files",
        python_callable=detect_new_files,
    )

    # Step 2: create ephemeral Spark cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-cleaning-{uuid.uuid4().hex[:8]}",
        cluster_description="Ephemeral Spark cluster for data cleaning",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_BUCKET_NAME,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=40,
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=128,
        datanode_count=3,
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS"],
        connection_id=YC_SA_CONNECTION.conn_id,
        security_group_ids=[DP_SECURITY_GROUP_ID],
    )

    # Step 3: run cleaning script on all files
    run_cleaning = DataprocCreatePysparkJobOperator(
        task_id="run_data_cleaning",
        main_python_file_uri=f"s3a://{S3_BUCKET_NAME}/src/data_cleaning.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=["--bucket", S3_BUCKET_NAME],
    )

    # Step 4: update manifest of processed files
    update_processed = PythonOperator(
        task_id="update_manifest",
        python_callable=update_manifest,
    )

    # Step 5: delete cluster (even if cleaning failed)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # DAG flow
    (
        setup_connections
        >> detect_new
        >> create_cluster
        >> run_cleaning
        >> update_processed
        >> delete_cluster
    )
