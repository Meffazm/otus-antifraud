"""
DAG: streaming_inference
Streaming inference pipeline: Kafka producer + Spark Structured Streaming.

Creates an ephemeral DataProc cluster, runs the Kafka producer (data replay)
and Spark Structured Streaming inference job, then cleans up.
"""

import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
    InitializationAction,
)

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

KAFKA_BOOTSTRAP = Variable.get("KAFKA_BOOTSTRAP")
KAFKA_PASSWORD = Variable.get("KAFKA_PASSWORD")

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


SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3"
PRODUCER_LIMIT = 100000  # messages to send (~1667 TPS * 60s)
STREAMING_DURATION = 60

with DAG(
    dag_id="streaming_inference",
    start_date=datetime(2026, 3, 21),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["streaming", "hw_08"],
) as dag:

    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=setup_airflow_connections,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-streaming-{uuid.uuid4().hex[:8]}",
        cluster_description="Ephemeral Spark cluster for streaming inference",
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
        initialization_actions=[
            InitializationAction(
                uri=f"s3a://{S3_BUCKET_NAME}/src/dataproc_init.sh",
                args=[],
                timeout=300,
            ),
        ],
    )

    run_producer = DataprocCreatePysparkJobOperator(
        task_id="run_kafka_producer",
        main_python_file_uri=f"{S3_SRC_BUCKET}/kafka_producer.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=[
            "--bootstrap-server", KAFKA_BOOTSTRAP,
            "--user", "producer",
            "--password", KAFKA_PASSWORD,
            "--input", f"s3a://{S3_BUCKET_NAME}/cleaned/",
            "--limit", str(PRODUCER_LIMIT),
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
        ],
        properties={
            "spark.submit.deployMode": "cluster",
        },
    )

    run_streaming = DataprocCreatePysparkJobOperator(
        task_id="run_streaming_inference",
        main_python_file_uri=f"{S3_SRC_BUCKET}/streaming_inference.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        args=[
            "--kafka-bootstrap", KAFKA_BOOTSTRAP,
            "--kafka-user", "consumer",
            "--kafka-password", KAFKA_PASSWORD,
            "--model-path", f"s3a://{S3_BUCKET_NAME}/models/model_20260321",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
            "--duration", str(STREAMING_DURATION),
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.jars.packages": SPARK_KAFKA_PACKAGE,
        },
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    setup_connections >> create_cluster >> run_producer >> run_streaming >> delete_cluster
