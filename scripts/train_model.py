"""
PySpark script for training anti-fraud model with MLflow logging.

Usage (via Airflow DAG — venv distributed via spark.yarn.dist.archives):
    spark-submit train_model.py \
        --input s3a://BUCKET/cleaned/ \
        --output s3a://BUCKET/models/model_YYYYMMDD \
        --tracking-uri http://MLFLOW_IP:5000 \
        --experiment-name antifraud_model \
        --s3-endpoint-url https://storage.yandexcloud.net \
        --s3-access-key KEY --s3-secret-key SECRET \
        --sample-fraction 0.01
"""

import os
import sys
import traceback
import argparse
from datetime import datetime

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


FEATURE_COLS = ["tx_amount", "hour_of_day", "day_of_week", "is_weekend", "is_night"]


def create_spark_session(s3_config=None):
    """Create Spark session with optional S3 configuration."""
    builder = SparkSession.builder.appName("antifraud-training")
    if s3_config:
        builder = (builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", s3_config["endpoint_url"])
            .config("spark.hadoop.fs.s3a.access.key", s3_config["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", s3_config["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )
    return builder.getOrCreate()


def create_features(df):
    """Create time-based features."""
    df = df.withColumn("hour_of_day", hour(col("tx_datetime")).cast("double"))
    df = df.withColumn("day_of_week", dayofweek(col("tx_datetime")).cast("double"))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1.0).otherwise(0.0))
    df = df.withColumn("is_night", when(col("hour_of_day").between(0, 6), 1.0).otherwise(0.0))
    df = df.withColumn("tx_amount", col("tx_amount").cast("double"))
    return df


def train_and_log(train_df, test_df, run_name):
    """Train model, log metrics and model to MLflow."""
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="tx_fraud",
                            maxIter=10, regParam=0.01)

    pipeline = Pipeline(stages=[assembler, lr])

    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="tx_fraud", metricName="areaUnderROC")
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="tx_fraud", predictionCol="prediction", metricName="f1")
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="tx_fraud", predictionCol="prediction", metricName="accuracy")

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id
        print(f"MLflow Run ID: {run_id}")

        mlflow.log_param("model", "LogisticRegression")
        mlflow.log_param("maxIter", 10)
        mlflow.log_param("regParam", 0.01)
        mlflow.log_param("features", ",".join(FEATURE_COLS))

        print("Training...")
        model = pipeline.fit(train_df)

        predictions = model.transform(test_df)

        auc = evaluator_auc.evaluate(predictions)
        f1 = evaluator_f1.evaluate(predictions)
        accuracy = evaluator_acc.evaluate(predictions)

        mlflow.log_metric("auc", auc)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("accuracy", accuracy)

        print(f"AUC={auc:.4f}, F1={f1:.4f}, Accuracy={accuracy:.4f}")

        print("Logging model to MLflow...")
        mlflow.spark.log_model(model, "model")

        return model, {"run_id": run_id, "auc": auc, "f1": f1, "accuracy": accuracy}


def compare_and_register(metrics, experiment_name):
    """Register model and promote to champion if better."""
    client = MlflowClient()
    model_name = f"{experiment_name}_model"

    # Create registered model if not exists
    try:
        client.get_registered_model(model_name)
    except Exception:
        client.create_registered_model(model_name)
        print(f"Created registered model '{model_name}'")

    # Register new version
    model_uri = f"runs:/{metrics['run_id']}/model"
    model_details = mlflow.register_model(model_uri, model_name)
    new_version = model_details.version
    print(f"Registered version {new_version}")

    # Check current champion
    try:
        if hasattr(client, 'set_registered_model_alias'):
            client.set_registered_model_alias(model_name, "champion", new_version)
        else:
            client.set_model_version_tag(model_name, new_version, "alias", "champion")
        print(f"Version {new_version} set as 'champion'")
    except Exception as e:
        print(f"Warning: could not set alias: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", default="antifraud_model")
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    parser.add_argument("--sample-fraction", type=float, default=0.001)
    parser.add_argument("--run-name", default=None)
    parser.add_argument("--auto-register", action="store_true")

    os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
    args = parser.parse_args()

    # S3 env vars for MLflow artifact store
    os.environ['AWS_ACCESS_KEY_ID'] = args.s3_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.s3_secret_key
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.s3_endpoint_url

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)
    print(f"MLflow: {args.tracking_uri}, experiment: {args.experiment_name}")

    s3_config = {
        "endpoint_url": args.s3_endpoint_url,
        "access_key": args.s3_access_key,
        "secret_key": args.s3_secret_key,
    }
    spark = create_spark_session(s3_config)

    try:
        # Read single parquet partition for speed
        input_path = args.input.rstrip("/")
        single_file = f"{input_path}/part-00000*"
        print(f"Loading from {single_file}")
        df = spark.read.parquet(single_file)

        df = create_features(df)
        df = df.select(FEATURE_COLS + ["tx_fraud"]).na.drop()
        print(f"Rows: {df.count()}")

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

        run_name = args.run_name or f"antifraud_lr_{datetime.now().strftime('%Y%m%d_%H%M')}"
        model, metrics = train_and_log(train_df, test_df, run_name)

        print(f"Saving model to {args.output}")
        model.write().overwrite().save(args.output)

        if args.auto_register:
            compare_and_register(metrics, args.experiment_name)

        print("Training completed successfully!")
    except Exception as e:
        print(f"ERROR: {e}")
        print(traceback.format_exc())
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
