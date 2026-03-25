"""
Kafka producer: replays cleaned transaction data from S3 into Kafka.
"""

import json
import time
import ssl
import os
import sys
import argparse

from pyspark.sql import SparkSession


KAFKA_CA_PATHS = [
    "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    "/tmp/YandexCA.pem",
]


def ensure_ca_cert():
    for path in KAFKA_CA_PATHS:
        if os.path.exists(path):
            return path
    dest = KAFKA_CA_PATHS[-1]
    try:
        import subprocess
        subprocess.run(
            ["wget", "-q", "https://storage.yandexcloud.net/cloud-certs/CA.pem",
             "-O", dest], check=True, timeout=10)
    except Exception:
        import urllib.request
        urllib.request.urlretrieve(
            "https://storage.yandexcloud.net/cloud-certs/CA.pem", dest)
    return dest


def main():
    parser = argparse.ArgumentParser(description="Kafka transaction producer")
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--user", default="producer")
    parser.add_argument("--password", required=True)
    parser.add_argument("--input", required=True, help="Path to parquet dir")
    parser.add_argument("--limit", type=int, default=1500, help="Number of messages to send")
    parser.add_argument("--s3-endpoint-url", default="https://storage.yandexcloud.net")
    parser.add_argument("--s3-access-key", default="")
    parser.add_argument("--s3-secret-key", default="")
    args = parser.parse_args()

    builder = SparkSession.builder.appName("kafka-producer")
    if args.s3_access_key:
        builder = (builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint_url)
            .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )
    spark = builder.getOrCreate()

    input_path = args.input.rstrip("/")
    df = spark.read.parquet(f"{input_path}/part-00000*")
    rows = df.collect()
    total_rows = len(rows)

    # Kafka - all within Spark context (no spark.stop() before this)
    ca_path = ensure_ca_cert()
    ssl_context = ssl.create_default_context(cafile=ca_path)

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=args.user,
        sasl_plain_password=args.password,
        ssl_context=ssl_context,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for i in range(args.limit):
        row = rows[i % total_rows]
        producer.send(args.topic, value={
            "tranaction_id": int(row["tranaction_id"]),
            "tx_datetime": str(row["tx_datetime"]),
            "customer_id": int(row["customer_id"]),
            "terminal_id": str(row["terminal_id"]),
            "tx_amount": float(row["tx_amount"]),
            "tx_time_seconds": int(row["tx_time_seconds"]),
            "tx_time_days": int(row["tx_time_days"]),
            "tx_fraud": int(row["tx_fraud"]),
            "tx_fraud_scenario": int(row["tx_fraud_scenario"]),
        })

    producer.flush()
    producer.close()
    spark.stop()


if __name__ == "__main__":
    main()
