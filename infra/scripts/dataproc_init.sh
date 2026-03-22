#!/bin/bash
# Install Yandex CA cert and create JKS truststore for Kafka SSL
set -e

CA_DIR="/usr/local/share/ca-certificates/Yandex"
CA_FILE="${CA_DIR}/YandexInternalRootCA.crt"
TRUSTSTORE="/etc/security/ssl/truststore.jks"
TRUSTSTORE_PASS="changeit"

mkdir -p "$CA_DIR" /etc/security/ssl

if [ ! -f "$CA_FILE" ]; then
    wget -q "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O "$CA_FILE"
    chmod 644 "$CA_FILE"
fi

if [ ! -f "$TRUSTSTORE" ]; then
    keytool -import -file "$CA_FILE" \
        -storepass "$TRUSTSTORE_PASS" \
        -alias yandex-ca \
        -keystore "$TRUSTSTORE" \
        -noprompt
    chmod 644 "$TRUSTSTORE"
fi

# Install kafka-python-ng for the producer script
# Install for all available Pythons (system + conda)
pip3 install --quiet kafka-python-ng 2>/dev/null || true
/opt/conda/bin/pip install --quiet kafka-python-ng 2>/dev/null || true
python3 -m pip install --quiet kafka-python-ng 2>/dev/null || true
