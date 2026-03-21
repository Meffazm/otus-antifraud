"""
DAG: model_training
Periodic model retraining with DataProc, PySpark, and MLflow.
"""

import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

# Variables
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"
S3_VENV_ARCHIVE = f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz"

DP_SA_ID = Variable.get("DP_SA_ID")
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")

# Connections
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)


def setup_airflow_connections(**kwargs):
    session = Session()
    try:
        for conn in [YC_SA_CONNECTION]:
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


with DAG(
    dag_id="model_training",
    start_date=datetime(2026, 3, 15),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
    },
) as dag:

    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=setup_airflow_connections,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-training-{uuid.uuid4().hex[:8]}",
        cluster_description="Ephemeral Spark cluster for model training",
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
        datanode_disk_size=50,
        datanode_count=1,
        computenode_count=0,
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
        security_group_ids=[DP_SECURITY_GROUP_ID],
    )

    train_model = DataprocCreatePysparkJobOperator(
        task_id="train_model",
        main_python_file_uri=f"{S3_SRC_BUCKET}/train_model.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=[
            "--input", f"s3a://{S3_BUCKET_NAME}/cleaned/",
            "--output", f"s3a://{S3_BUCKET_NAME}/models/model_{datetime.now().strftime('%Y%m%d')}",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", "antifraud_model",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
            "--run-name", f"training_{datetime.now().strftime('%Y%m%d_%H%M')}",
            "--auto-register",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"{S3_VENV_ARCHIVE}#.venv",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./.venv/bin/python3",
            "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "./.venv/bin/python3",
        },
    )

    validate_model = DataprocCreatePysparkJobOperator(
        task_id="validate_model",
        main_python_file_uri=f"{S3_SRC_BUCKET}/validate_model.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=[
            "--input", f"s3a://{S3_BUCKET_NAME}/cleaned/",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", "antifraud_model",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
            "--run-name", f"validation_{datetime.now().strftime('%Y%m%d_%H%M')}",
            "--auto-deploy",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"{S3_VENV_ARCHIVE}#.venv",
            "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./.venv/bin/python3",
            "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON": "./.venv/bin/python3",
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    setup_connections >> create_cluster >> train_model >> validate_model >> delete_cluster
