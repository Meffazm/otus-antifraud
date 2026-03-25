"""
Spark Structured Streaming inference: reads transactions from Kafka,
applies the trained anti-fraud PySpark ML pipeline, writes predictions
to a Kafka output topic.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
        streaming_inference.py \
        --kafka-bootstrap rc1a-xxx.mdb.yandexcloud.net:9091 \
        --kafka-user consumer --kafka-password SECRET \
        --model-path s3a://BUCKET/models/model_YYYYMMDD \
        --s3-endpoint-url https://storage.yandexcloud.net \
        --s3-access-key KEY --s3-secret-key SECRET \
        --duration 180
"""

import os
import sys
import subprocess
import argparse
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp,
    hour, dayofweek, when, to_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, DoubleType, IntegerType,
)
from pyspark.ml import PipelineModel


TRANSACTION_SCHEMA = StructType([
    StructField("tranaction_id", LongType()),
    StructField("tx_datetime", StringType()),
    StructField("customer_id", LongType()),
    StructField("terminal_id", StringType()),
    StructField("tx_amount", DoubleType()),
    StructField("tx_time_seconds", LongType()),
    StructField("tx_time_days", LongType()),
    StructField("tx_fraud", IntegerType()),
    StructField("tx_fraud_scenario", IntegerType()),
])

FEATURE_COLS = ["tx_amount", "hour_of_day", "day_of_week", "is_weekend", "is_night"]


def create_features(df):
    """Create time-based features (same as train_model.py)."""
    df = df.withColumn("tx_ts", to_timestamp(col("tx_datetime")))
    df = df.withColumn("hour_of_day", hour(col("tx_ts")).cast("double"))
    df = df.withColumn("day_of_week", dayofweek(col("tx_ts")).cast("double"))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1.0).otherwise(0.0))
    df = df.withColumn("is_night", when(col("hour_of_day").between(0, 6), 1.0).otherwise(0.0))
    df = df.withColumn("tx_amount", col("tx_amount").cast("double"))
    return df


def main():
    parser = argparse.ArgumentParser(description="Streaming inference")
    parser.add_argument("--kafka-bootstrap", required=True)
    parser.add_argument("--kafka-user", default="consumer")
    parser.add_argument("--kafka-password", required=True)
    parser.add_argument("--input-topic", default="transactions")
    parser.add_argument("--output-topic", default="predictions")
    parser.add_argument("--model-path", required=True, help="S3 path to PySpark PipelineModel")
    parser.add_argument("--s3-endpoint-url", default="https://storage.yandexcloud.net")
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    parser.add_argument("--duration", type=int, default=180,
                        help="How long to run streaming (seconds)")
    parser.add_argument("--checkpoint", default="/tmp/streaming-checkpoint-" + str(int(__import__('time').time())))
    args = parser.parse_args()

    # Find or create JKS truststore for Kafka SSL
    truststore_pass = "changeit"
    truststore_path = None
    for tp in ["/etc/security/ssl/truststore.jks", "/tmp/kafka-truststore.jks"]:
        if os.path.exists(tp):
            truststore_path = tp
            break
    if not truststore_path:
        ca_path = "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt"
        if not os.path.exists(ca_path):
            ca_path = "/tmp/YandexCA.pem"
            subprocess.run(["wget", "-q", "https://storage.yandexcloud.net/cloud-certs/CA.pem",
                             "-O", ca_path], check=True)
        truststore_path = "/tmp/kafka-truststore.jks"
        subprocess.run(["keytool", "-import", "-file", ca_path,
                         "-storepass", truststore_pass, "-alias", "yandex-ca",
                         "-keystore", truststore_path, "-noprompt"], check=True)

    kafka_jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.kafka_user}" password="{args.kafka_password}";'
    )

    spark = (SparkSession.builder
        .appName("antifraud-streaming-inference")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .getOrCreate()
    )

    try:
        print(f"Loading model from {args.model_path}")
        model = PipelineModel.load(args.model_path)
        print("Model loaded successfully")

        kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option("kafka.sasl.jaas.config", kafka_jaas)
            .option("kafka.ssl.truststore.location", truststore_path)
            .option("kafka.ssl.truststore.password", truststore_pass)
            .option("subscribe", args.input_topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        parsed_df = (kafka_df
            .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
            .select(
                from_json(col("json_str"), TRANSACTION_SCHEMA).alias("data"),
                col("kafka_ts"),
            )
            .select("data.*", "kafka_ts")
        )

        featured_df = create_features(parsed_df)
        featured_df = featured_df.filter(col("tranaction_id").isNotNull())
        predictions_df = model.transform(featured_df)

        output_df = predictions_df.select(
            to_json(struct(
                col("tranaction_id"),
                col("customer_id"),
                col("terminal_id"),
                col("tx_amount"),
                col("tx_fraud").alias("ground_truth"),
                col("prediction").cast("int").alias("prediction"),
                col("kafka_ts").alias("event_time"),
                current_timestamp().alias("scored_at"),
            )).alias("value")
        )

        query = (output_df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
            .option("kafka.sasl.jaas.config", kafka_jaas)
            .option("kafka.ssl.truststore.location", truststore_path)
            .option("kafka.ssl.truststore.password", truststore_pass)
            .option("topic", args.output_topic)
            .option("checkpointLocation", args.checkpoint)
            .outputMode("append")
            .start()
        )

        print(f"Streaming started. Will run for {args.duration}s...")
        query.awaitTermination(timeout=args.duration)
        query.stop()
        print("Streaming inference completed")

    except Exception as e:
        error_msg = f"ERROR: {e}\n{traceback.format_exc()}"
        print(error_msg)
        # Write error to S3 so we can read it
        try:
            error_rdd = spark.sparkContext.parallelize([error_msg])
            error_rdd.saveAsTextFile(f"{args.model_path.rsplit('/', 1)[0]}/streaming_error_{int(__import__('time').time())}")
        except Exception:
            pass
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
