#!/bin/bash
# Cloud-init script for MLflow VM
# Installs MLflow, connects to Managed PostgreSQL and S3 artifact store

set -e

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Installing system packages..."
apt-get update -qq
apt-get install -y -qq python3-pip

log "Installing MLflow and dependencies..."
pip3 install mlflow boto3 psycopg2-binary

log "Configuring MLflow environment..."
cat > /etc/default/mlflow << 'ENVEOF'
AWS_ACCESS_KEY_ID=${s3_access_key}
AWS_SECRET_ACCESS_KEY=${s3_secret_key}
MLFLOW_S3_ENDPOINT_URL=https://storage.yandexcloud.net
MLFLOW_ALLOW_INBOUND_HEADERS=true
ENVEOF

log "Creating MLflow systemd service..."
cat > /etc/systemd/system/mlflow.service << 'SVCEOF'
[Unit]
Description=MLflow Tracking Server
After=network.target

[Service]
Type=simple
EnvironmentFile=/etc/default/mlflow
ExecStart=/usr/local/bin/mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --allowed-hosts "*" \
    --no-serve-artifacts \
    --disable-security-middleware \
    --backend-store-uri postgresql://mlflow:${db_password}@${db_host}:6432/mlflow \
    --default-artifact-root s3://${s3_bucket_name}/mlflow-artifacts/
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SVCEOF

log "Starting MLflow service..."
systemctl daemon-reload
systemctl enable mlflow
systemctl start mlflow

log "MLflow setup complete. Server running on port 5000."
