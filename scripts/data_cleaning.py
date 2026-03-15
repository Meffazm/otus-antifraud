"""
Data cleaning script for anti-fraud transaction dataset.

Reads raw CSV files from S3, removes/fixes data quality issues,
and saves cleaned data as parquet.

Usage:
    spark-submit scripts/data_cleaning.py <input_path> <output_path>

Example:
    spark-submit scripts/data_cleaning.py \
        s3a://otus-mlops-data-b1g7vl7oovirupu7q3vf/*.txt \
        s3a://otus-mlops-data-b1g7vl7oovirupu7q3vf/cleaned/
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DoubleType, IntegerType,
)


def create_spark_session():
    return SparkSession.builder \
        .appName("data-cleaning") \
        .getOrCreate()


def read_raw_data(spark, input_path):
    """Read raw CSV files with explicit schema."""
    schema = StructType([
        StructField("tranaction_id", LongType(), True),
        StructField("tx_datetime", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("terminal_id", StringType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_time_seconds", LongType(), True),
        StructField("tx_time_days", LongType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
    ])

    return spark.read.csv(
        input_path,
        schema=schema,
        header=False,
        comment="#",
    )


def clean_data(df):
    """Apply all cleaning transformations (lazy — no actions triggered)."""

    # 1-2. Remove NULL and non-numeric terminal_id ('Err'), cast to int
    df = df.filter(col("terminal_id").cast(IntegerType()).isNotNull())
    df = df.withColumn("terminal_id", col("terminal_id").cast(IntegerType()))

    # 3. Remove negative customer_id
    df = df.filter(col("customer_id") >= 0)

    # 4. Fix invalid dates: '24:00:00' → '00:00:00'
    df = df.withColumn(
        "tx_datetime",
        regexp_replace(col("tx_datetime"), " 24:00:00$", " 00:00:00")
    )

    # 5. Parse tx_datetime and remove unparseable
    df = df.withColumn("tx_datetime", to_timestamp(col("tx_datetime")))
    df = df.filter(col("tx_datetime").isNotNull())

    # 6. Remove zero tx_amount
    df = df.filter(col("tx_amount") > 0)

    # 7. Remove duplicate transaction IDs
    df = df.dropDuplicates(["tranaction_id"])

    return df


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit data_cleaning.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = create_spark_session()

    print(f"Reading data from: {input_path}")
    df = read_raw_data(spark, input_path)

    print("Cleaning data...")
    df_clean = clean_data(df)

    print(f"Saving cleaned data to: {output_path}")
    df_clean.write.parquet(output_path, mode="overwrite")
    print("Done!")

    spark.stop()


if __name__ == "__main__":
    main()
