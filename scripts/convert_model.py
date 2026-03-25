"""
Convert PySpark PipelineModel from S3 into a lightweight JSON file.

Reads the VectorAssembler metadata (feature names) and
LogisticRegressionModel parquet (coefficients + intercept) without
requiring PySpark — only boto3 and pyarrow.

Usage:
    python scripts/convert_model.py \
        --bucket my-bucket \
        --model-path models/model_20260321 \
        --output model/model.json \
        --endpoint-url https://storage.yandexcloud.net
"""

import os
import sys
import json
import argparse
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq


# ── CLI ─────────────────────────────────────────────────────────────────


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Convert PySpark PipelineModel (S3) to JSON coefficients file."
    )
    parser.add_argument(
        "--bucket", required=True, help="S3 bucket name"
    )
    parser.add_argument(
        "--model-path", required=True,
        help="Path prefix in the bucket, e.g. models/model_20260321"
    )
    parser.add_argument(
        "--output", default="model/model.json",
        help="Output JSON file path (default: model/model.json)"
    )
    parser.add_argument(
        "--endpoint-url", default="https://storage.yandexcloud.net",
        help="S3 endpoint URL (default: https://storage.yandexcloud.net)"
    )
    parser.add_argument(
        "--access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help="S3 access key (default: env AWS_ACCESS_KEY_ID)"
    )
    parser.add_argument(
        "--secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help="S3 secret key (default: env AWS_SECRET_ACCESS_KEY)"
    )
    return parser.parse_args()


# ── S3 helpers ──────────────────────────────────────────────────────────


def create_s3_client(endpoint_url, access_key, secret_key):
    """Create and return an S3 client."""
    if not access_key or not secret_key:
        print("ERROR: S3 credentials not provided. "
              "Use --access-key/--secret-key or set "
              "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars.")
        sys.exit(1)

    print(f"Connecting to S3 at {endpoint_url} ...")
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def list_objects(s3, bucket, prefix):
    """List all object keys under a given prefix."""
    keys = []
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            continuation_token = resp["NextContinuationToken"]
        else:
            break
    return keys


def download_bytes(s3, bucket, key):
    """Download an S3 object and return its bytes."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


# ── Stage discovery ─────────────────────────────────────────────────────


def find_stages(s3, bucket, model_path):
    """
    Discover VectorAssembler and LogisticRegressionModel stage prefixes
    under {model_path}/stages/.
    Returns (assembler_prefix, lr_prefix).
    """
    stages_prefix = f"{model_path.rstrip('/')}/stages/"
    keys = list_objects(s3, bucket, stages_prefix)

    if not keys:
        print(f"ERROR: No objects found under s3://{bucket}/{stages_prefix}")
        sys.exit(1)

    assembler_prefix = None
    lr_prefix = None

    # Extract unique stage directory names (first path component after stages/)
    stage_dirs = set()
    for key in keys:
        relative = key[len(stages_prefix):]
        stage_dir = relative.split("/")[0]
        stage_dirs.add(stage_dir)

    for stage_dir in sorted(stage_dirs):
        lower = stage_dir.lower()
        if "vectorassembler" in lower:
            assembler_prefix = f"{stages_prefix}{stage_dir}"
        elif "logisticregression" in lower:
            lr_prefix = f"{stages_prefix}{stage_dir}"

    if not assembler_prefix:
        print("ERROR: VectorAssembler stage not found in model stages.")
        sys.exit(1)
    if not lr_prefix:
        print("ERROR: LogisticRegressionModel stage not found in model stages.")
        sys.exit(1)

    print(f"  Found VectorAssembler: {assembler_prefix}")
    print(f"  Found LogisticRegression: {lr_prefix}")
    return assembler_prefix, lr_prefix


# ── Feature names from VectorAssembler ──────────────────────────────────


def extract_feature_names(s3, bucket, assembler_prefix):
    """Read VectorAssembler metadata and return the list of input column names."""
    metadata_key = f"{assembler_prefix}/metadata/part-00000"
    print(f"Downloading VectorAssembler metadata: {metadata_key} ...")
    raw = download_bytes(s3, bucket, metadata_key)

    try:
        meta = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"ERROR: Failed to parse VectorAssembler metadata JSON: {exc}")
        sys.exit(1)

    # paramMap may be at the top level or nested
    param_map = meta.get("paramMap", {})
    input_cols = param_map.get("inputCols")
    if not input_cols:
        print("ERROR: 'inputCols' not found in VectorAssembler paramMap.")
        print(f"  Metadata content: {meta}")
        sys.exit(1)

    print(f"  Feature names: {input_cols}")
    return input_cols


# ── Coefficients from LogisticRegressionModel ───────────────────────────


def _dense_matrix_to_list(mat):
    """
    Convert a DenseMatrix struct dict to a flat list of coefficients.
    For a 1×N coefficient matrix the values are directly the coefficients.
    """
    values = list(mat["values"])
    num_rows = mat["numRows"]
    num_cols = mat["numCols"]
    is_transposed = mat.get("isTransposed", False)

    # For binary classification: 1 x numFeatures
    if num_rows == 1:
        return values[:num_cols]

    # If transposed, values are in row-major; otherwise column-major
    if is_transposed:
        # row-major: first row is values[0:numCols]
        return values[:num_cols]
    else:
        # column-major: row 0 values are at indices 0, numRows, 2*numRows, ...
        return [values[c * num_rows] for c in range(num_cols)]


def _sparse_matrix_to_list(mat, num_features):
    """
    Convert a SparseMatrix (CSC format) struct dict to a dense list of
    coefficients for the first row.
    """
    col_ptrs = list(mat["colPtrs"])
    row_indices = list(mat["rowIndices"])
    values = list(mat["values"])
    num_rows = mat["numRows"]
    num_cols = mat["numCols"]
    is_transposed = mat.get("isTransposed", False)

    if is_transposed:
        # Transposed sparse matrix: CSC on the transposed matrix means
        # "columns" are the original rows.  For binary LR there is 1 row,
        # so colPtrs has length 2 and all values belong to row 0.
        return list(values[:num_features])

    # Standard CSC: iterate over columns, pick row-0 entries
    coeffs = [0.0] * num_cols
    for c in range(num_cols):
        start, end = col_ptrs[c], col_ptrs[c + 1]
        for idx in range(start, end):
            if row_indices[idx] == 0:
                coeffs[c] = values[idx]
    return coeffs[:num_features]


def _vector_to_float(vec):
    """
    Extract a single float from a DenseVector or SparseVector struct dict.
    Used for the intercept (size-1 vector in binary classification).
    """
    vec_type = vec.get("type")
    values = list(vec["values"])

    if vec_type == 1:
        # DenseVector
        return float(values[0]) if values else 0.0
    else:
        # SparseVector — value at the single non-zero index
        return float(values[0]) if values else 0.0


def extract_coefficients(s3, bucket, lr_prefix):
    """
    Download the LR model parquet and return (coefficients_list, intercept_float).
    """
    # Find parquet file(s) under lr_prefix/data/
    data_prefix = f"{lr_prefix}/data/"
    keys = list_objects(s3, bucket, data_prefix)
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        print(f"ERROR: No parquet files found under s3://{bucket}/{data_prefix}")
        sys.exit(1)

    parquet_key = parquet_keys[0]
    print(f"Downloading LR model parquet: {parquet_key} ...")

    # Download to a temp file so pyarrow can read it
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
        raw = download_bytes(s3, bucket, parquet_key)
        tmp.write(raw)

    try:
        table = pq.read_table(tmp_path)
        df = table.to_pydict()
    finally:
        os.unlink(tmp_path)

    num_features = int(df["numFeatures"][0])
    print(f"  numClasses={df['numClasses'][0]}, numFeatures={num_features}")

    # --- Coefficient matrix ---
    coeff_raw = df["coefficientMatrix"][0]
    # pyarrow may return it as a dict-like struct
    coeff = dict(coeff_raw) if not isinstance(coeff_raw, dict) else coeff_raw

    mat_type = coeff.get("type")
    if mat_type == 1:
        # DenseMatrix
        coefficients = _dense_matrix_to_list(coeff)
    elif mat_type == 0:
        # SparseMatrix (CSC)
        coefficients = _sparse_matrix_to_list(coeff, num_features)
    else:
        print(f"ERROR: Unknown coefficient matrix type: {mat_type}")
        sys.exit(1)

    # Ensure we have exactly num_features coefficients
    coefficients = [float(c) for c in coefficients[:num_features]]

    # --- Intercept vector ---
    intercept_raw = df["interceptVector"][0]
    intercept_vec = dict(intercept_raw) if not isinstance(intercept_raw, dict) else intercept_raw
    intercept = _vector_to_float(intercept_vec)

    print(f"  Coefficients: {coefficients}")
    print(f"  Intercept: {intercept}")
    return coefficients, intercept


# ── Output ──────────────────────────────────────────────────────────────


def save_json(feature_names, coefficients, intercept, output_path):
    """Write the model JSON to disk."""
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    payload = {
        "feature_names": feature_names,
        "coefficients": coefficients,
        "intercept": intercept,
    }

    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    print(f"Model saved to {output_path}")


# ── Main ────────────────────────────────────────────────────────────────


def main():
    args = parse_args()

    s3 = create_s3_client(args.endpoint_url, args.access_key, args.secret_key)

    print(f"Reading model from s3://{args.bucket}/{args.model_path} ...")
    assembler_prefix, lr_prefix = find_stages(s3, args.bucket, args.model_path)

    feature_names = extract_feature_names(s3, args.bucket, assembler_prefix)
    coefficients, intercept = extract_coefficients(s3, args.bucket, lr_prefix)

    if len(feature_names) != len(coefficients):
        print(f"WARNING: feature count mismatch — "
              f"{len(feature_names)} names vs {len(coefficients)} coefficients. "
              f"Using min of the two.")
        n = min(len(feature_names), len(coefficients))
        feature_names = feature_names[:n]
        coefficients = coefficients[:n]

    save_json(feature_names, coefficients, intercept, args.output)
    print("Done.")


if __name__ == "__main__":
    main()
