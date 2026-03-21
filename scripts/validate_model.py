"""
A/B test validation script for anti-fraud model.

Compares the current champion model against a newly trained candidate
using bootstrap resampling and statistical tests.

Usage:
    spark-submit validate_model.py \
        --input s3a://BUCKET/cleaned/ \
        --tracking-uri http://MLFLOW_IP:5000 \
        --experiment-name antifraud_model \
        --s3-endpoint-url https://storage.yandexcloud.net \
        --s3-access-key KEY --s3-secret-key SECRET
"""

import os
import sys
import math
import random
import traceback
import argparse
import json
import urllib.request
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, when
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient


FEATURE_COLS = ["tx_amount", "hour_of_day", "day_of_week", "is_weekend", "is_night"]


def create_spark_session(s3_config=None):
    builder = SparkSession.builder.appName("antifraud-validation")
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
    df = df.withColumn("hour_of_day", hour(col("tx_datetime")).cast("double"))
    df = df.withColumn("day_of_week", dayofweek(col("tx_datetime")).cast("double"))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1.0).otherwise(0.0))
    df = df.withColumn("is_night", when(col("hour_of_day").between(0, 6), 1.0).otherwise(0.0))
    df = df.withColumn("tx_amount", col("tx_amount").cast("double"))
    return df


def evaluate_model(model, test_df):
    """Evaluate model and return metrics dict."""
    predictions = model.transform(test_df)
    auc_eval = BinaryClassificationEvaluator(labelCol="tx_fraud", metricName="areaUnderROC")
    f1_eval = MulticlassClassificationEvaluator(labelCol="tx_fraud", predictionCol="prediction", metricName="f1")
    acc_eval = MulticlassClassificationEvaluator(labelCol="tx_fraud", predictionCol="prediction", metricName="accuracy")

    return {
        "auc": auc_eval.evaluate(predictions),
        "f1": f1_eval.evaluate(predictions),
        "accuracy": acc_eval.evaluate(predictions),
    }


def bootstrap_evaluate(model, test_df, n_iterations=50):
    """Bootstrap evaluation — resample test set and compute metrics n times."""
    results = []
    total = test_df.count()

    for i in range(n_iterations):
        sample = test_df.sample(withReplacement=True, fraction=1.0, seed=i)
        metrics = evaluate_model(model, sample)
        results.append(metrics)
        if (i + 1) % 10 == 0:
            print(f"  Bootstrap iteration {i+1}/{n_iterations}")

    return results


def t_test(scores_a, scores_b):
    """Independent two-sample t-test (no scipy needed)."""
    n_a, n_b = len(scores_a), len(scores_b)
    mean_a = sum(scores_a) / n_a
    mean_b = sum(scores_b) / n_b
    var_a = sum((x - mean_a) ** 2 for x in scores_a) / (n_a - 1)
    var_b = sum((x - mean_b) ** 2 for x in scores_b) / (n_b - 1)

    se = math.sqrt(var_a / n_a + var_b / n_b)
    if se == 0:
        return 0.0, 1.0

    t_stat = (mean_b - mean_a) / se

    # Degrees of freedom (Welch's approximation)
    df = ((var_a / n_a + var_b / n_b) ** 2 /
          ((var_a / n_a) ** 2 / (n_a - 1) + (var_b / n_b) ** 2 / (n_b - 1)))

    # Approximate p-value using normal distribution (good for df > 30)
    # P(|T| > |t|) ≈ 2 * (1 - Φ(|t|))
    z = abs(t_stat)
    # Approximation of standard normal CDF
    p_value = 2.0 * (1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))

    return t_stat, p_value


def cohens_d(scores_a, scores_b):
    """Cohen's d effect size."""
    mean_a = sum(scores_a) / len(scores_a)
    mean_b = sum(scores_b) / len(scores_b)
    var_a = sum((x - mean_a) ** 2 for x in scores_a) / (len(scores_a) - 1)
    var_b = sum((x - mean_b) ** 2 for x in scores_b) / (len(scores_b) - 1)
    pooled_std = math.sqrt((var_a + var_b) / 2)
    if pooled_std == 0:
        return 0.0
    return (mean_b - mean_a) / pooled_std


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--experiment-name", default="antifraud_model")
    parser.add_argument("--s3-endpoint-url", required=True)
    parser.add_argument("--s3-access-key", required=True)
    parser.add_argument("--s3-secret-key", required=True)
    parser.add_argument("--bootstrap-iterations", type=int, default=10)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--auto-deploy", action="store_true")
    parser.add_argument("--run-name", default=None)

    os.environ['GIT_PYTHON_REFRESH'] = 'quiet'
    args = parser.parse_args()

    os.environ['AWS_ACCESS_KEY_ID'] = args.s3_access_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = args.s3_secret_key
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = args.s3_endpoint_url

    mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)
    client = MlflowClient()

    s3_config = {
        "endpoint_url": args.s3_endpoint_url,
        "access_key": args.s3_access_key,
        "secret_key": args.s3_secret_key,
    }
    spark = create_spark_session(s3_config)

    try:
        # Load test data (single parquet partition, sample 100K rows)
        input_path = args.input.rstrip("/")
        single_file = f"{input_path}/part-00001*"
        print(f"Loading test data from {single_file}")
        df = spark.read.parquet(single_file)
        df = create_features(df)
        df = df.select(FEATURE_COLS + ["tx_fraud"]).na.drop()
        total = df.count()
        if total > 100000:
            df = df.limit(100000)
            print(f"Sampled 100K from {total} rows")
        else:
            print(f"Test data: {total} rows")
        df.cache()
        df.count()  # trigger cache

        # Find champion and latest challenger
        model_name = f"{args.experiment_name}_model"
        print(f"Looking for registered model: {model_name}")

        try:
            all_versions = client.search_model_versions(f"name='{model_name}'")
        except Exception:
            print("No registered model found. Nothing to validate.")
            spark.stop()
            return

        if len(all_versions) < 2:
            print(f"Only {len(all_versions)} model version(s). Nothing to validate yet. Skipping.")
            spark.stop()
            return

        # Get latest two versions for comparison
        versions = sorted(all_versions, key=lambda v: int(v.version), reverse=True)
        latest = versions[0]

        print(f"Latest version: {latest.version} (run_id: {latest.run_id})")

        # Load latest model
        model_uri = f"models:/{model_name}/{latest.version}"
        print(f"Loading model from {model_uri}")
        candidate_model = mlflow.spark.load_model(model_uri)

        if len(versions) >= 2:
            previous = versions[1]
            print(f"Previous version: {previous.version} (run_id: {previous.run_id})")
            prev_uri = f"models:/{model_name}/{previous.version}"
            champion_model = mlflow.spark.load_model(prev_uri)
        else:
            print("Only one version exists — validating without comparison")
            champion_model = None

        run_name = args.run_name or f"validation_{datetime.now().strftime('%Y%m%d_%H%M')}"

        with mlflow.start_run(run_name=run_name) as run:
            run_id = run.info.run_id
            print(f"MLflow validation run: {run_id}")

            # Bootstrap evaluate candidate
            print(f"Bootstrap evaluating candidate ({args.bootstrap_iterations} iterations)...")
            candidate_bootstrap = bootstrap_evaluate(candidate_model, df, args.bootstrap_iterations)

            candidate_aucs = [m["auc"] for m in candidate_bootstrap]
            candidate_f1s = [m["f1"] for m in candidate_bootstrap]

            mean_auc = sum(candidate_aucs) / len(candidate_aucs)
            mean_f1 = sum(candidate_f1s) / len(candidate_f1s)

            mlflow.log_metric("candidate_mean_auc", mean_auc)
            mlflow.log_metric("candidate_mean_f1", mean_f1)
            mlflow.log_param("bootstrap_iterations", args.bootstrap_iterations)
            mlflow.log_param("alpha", args.alpha)
            mlflow.log_param("candidate_version", latest.version)

            if champion_model is not None:
                # Bootstrap evaluate champion
                print(f"Bootstrap evaluating champion ({args.bootstrap_iterations} iterations)...")
                champion_bootstrap = bootstrap_evaluate(champion_model, df, args.bootstrap_iterations)

                champion_aucs = [m["auc"] for m in champion_bootstrap]
                champion_f1s = [m["f1"] for m in champion_bootstrap]

                mlflow.log_metric("champion_mean_auc", sum(champion_aucs) / len(champion_aucs))
                mlflow.log_metric("champion_mean_f1", sum(champion_f1s) / len(champion_f1s))
                mlflow.log_param("champion_version", previous.version)

                # Statistical comparison on F1
                t_stat, p_value = t_test(champion_f1s, candidate_f1s)
                effect = cohens_d(champion_f1s, candidate_f1s)
                improvement = mean_f1 - sum(champion_f1s) / len(champion_f1s)

                mlflow.log_metric("t_statistic", t_stat)
                mlflow.log_metric("p_value", p_value)
                mlflow.log_metric("effect_size_cohens_d", effect)
                mlflow.log_metric("f1_improvement", improvement)

                is_significant = p_value < args.alpha
                should_deploy = is_significant and improvement > 0

                mlflow.log_param("is_significant", str(is_significant))
                mlflow.log_param("should_deploy", str(should_deploy))

                print(f"\n=== A/B Test Results ===")
                print(f"Champion F1: {sum(champion_f1s)/len(champion_f1s):.4f}")
                print(f"Candidate F1: {mean_f1:.4f}")
                print(f"Improvement: {improvement:.4f}")
                print(f"t-statistic: {t_stat:.4f}")
                print(f"p-value: {p_value:.6f}")
                print(f"Cohen's d: {effect:.4f}")
                print(f"Significant (p < {args.alpha}): {is_significant}")
                print(f"Should deploy: {should_deploy}")

                if should_deploy and args.auto_deploy:
                    try:
                        client.set_registered_model_alias(model_name, "champion", latest.version)
                        print(f"Deployed version {latest.version} as champion")
                    except Exception as e:
                        client.set_model_version_tag(model_name, latest.version, "alias", "champion")
                        print(f"Deployed version {latest.version} as champion (via tag)")

        print("Validation complete!")

    except Exception as e:
        print(f"ERROR: {e}")
        print(traceback.format_exc())
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
