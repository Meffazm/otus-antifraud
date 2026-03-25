#!/bin/bash
# Create a Python venv archive for DataProc nodes
# Must be run on a machine with Python 3.8 (matching DataProc 2.0)
# Or on the MLflow VM which has Ubuntu 22.04

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="/tmp/dp_venv"
OUTPUT_DIR="${SCRIPT_DIR}/../venvs"

echo "Creating venv..."
python3 -m venv "$VENV_DIR"
source "$VENV_DIR/bin/activate"

pip install --upgrade pip
pip install wheel
pip install pyspark==3.0.3 mlflow==2.17.2 boto3 psycopg2-binary

pip install venv-pack
mkdir -p "$OUTPUT_DIR"
venv-pack -o "$OUTPUT_DIR/venv.tar.gz"

deactivate
rm -rf "$VENV_DIR"

echo "Done: $OUTPUT_DIR/venv.tar.gz"
