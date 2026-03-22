init:
	terraform init -upgrade

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

# Destroy Airflow, MLflow, PostgreSQL, Kafka, K8s, and network; keep S3 bucket and SA
destroy-infra:
	terraform destroy -auto-approve \
		-target=yandex_kubernetes_node_group.k8s_nodes \
		-target=yandex_kubernetes_cluster.k8s \
		-target=yandex_container_registry.registry \
		-target=yandex_airflow_cluster.airflow \
		-target=yandex_compute_instance.mlflow \
		-target=yandex_mdb_postgresql_cluster.mlflow_db \
		-target=yandex_mdb_kafka_cluster.kafka \
		-target=yandex_vpc_security_group.security_group \
		-target=yandex_vpc_subnet.subnet \
		-target=yandex_vpc_route_table.route_table \
		-target=yandex_vpc_gateway.nat_gateway \
		-target=yandex_vpc_network.network \
		-target=yandex_iam_service_account_key.sa_auth_key \
		-target=local_file.airflow_variables

# K8s credentials
kubeconfig:
	yc managed-kubernetes cluster get-credentials $(shell terraform output -raw k8s_cluster_id) --external --force

# Destroy K8s + Registry (keep S3, SA, and other services)
destroy-k8s:
	terraform destroy -auto-approve \
		-target=yandex_kubernetes_node_group.k8s_nodes \
		-target=yandex_kubernetes_cluster.k8s \
		-target=yandex_container_registry.registry

BUCKET_NAME = $(shell terraform output -raw bucket_name 2>/dev/null)
MLFLOW_HOST = $(shell terraform output -raw mlflow_url 2>/dev/null | sed 's|http://||;s|:5000||')
KAFKA_BOOTSTRAP = $(shell terraform output -raw kafka_bootstrap 2>/dev/null)

# Upload DAGs and scripts to S3 bucket (Airflow syncs from there)
upload-dags:
	s3cmd put --recursive ../dags/ s3://$(BUCKET_NAME)/dags/

upload-scripts:
	s3cmd put ../scripts/data_cleaning.py s3://$(BUCKET_NAME)/src/data_cleaning.py
	s3cmd put ../scripts/train_model.py s3://$(BUCKET_NAME)/src/train_model.py
	s3cmd put ../scripts/validate_model.py s3://$(BUCKET_NAME)/src/validate_model.py
	s3cmd put ../scripts/kafka_producer.py s3://$(BUCKET_NAME)/src/kafka_producer.py
	s3cmd put ../scripts/streaming_inference.py s3://$(BUCKET_NAME)/src/streaming_inference.py
	s3cmd put scripts/dataproc_init.sh s3://$(BUCKET_NAME)/src/dataproc_init.sh
	-s3cmd put ../../hw_06/Excersise/mlflow-spark-1.27.0.jar s3://$(BUCKET_NAME)/src/mlflow-spark-1.27.0.jar

upload-venv:
	scp ubuntu@$(MLFLOW_HOST):/tmp/venvs/venv.tar.gz /tmp/venv.tar.gz
	s3cmd put /tmp/venv.tar.gz s3://$(BUCKET_NAME)/venvs/venv.tar.gz

upload-all: upload-dags upload-scripts

# Rebuild venv on MLflow VM with kafka-python-ng, then upload to S3
rebuild-venv:
	ssh ubuntu@$(MLFLOW_HOST) '\
		sudo apt-get install -y -qq python3-venv && \
		rm -rf /tmp/venv_build && mkdir /tmp/venv_build && \
		python3 -m venv --copies /tmp/venv_build/venv && \
		/tmp/venv_build/venv/bin/pip install --quiet mlflow boto3 pyspark kafka-python-ng && \
		mkdir -p /tmp/venvs && \
		tar -czf /tmp/venvs/venv.tar.gz -C /tmp/venv_build venv && \
		echo "venv rebuilt OK"'
	$(MAKE) upload-venv

# Deploy everything: apply terraform, upload artifacts, rebuild venv, show urls
deploy: apply upload-all airflow-vars

# Show all service URLs
urls:
	@echo "Airflow URL:  $(shell terraform output -raw airflow_url 2>/dev/null)"
	@echo "MLflow URL:   $(shell terraform output -raw mlflow_url 2>/dev/null)"
	@echo "Kafka:        $(KAFKA_BOOTSTRAP)"

# Import Airflow variables from generated JSON
airflow-vars:
	@echo "Import airflow_variables.json into Airflow UI:"
	@echo "  Admin -> Variables -> Import Variables -> select airflow_variables.json"
	@echo ""
	@$(MAKE) urls

# Check Kafka cluster status
kafka-status:
	yc managed-kafka cluster get otus-kafka --format json 2>/dev/null | python3 -c \
		"import json,sys;c=json.load(sys.stdin);print(f\"Status: {c['status']}\nHealth: {c.get('health','N/A')}\")"

# List Kafka topics
kafka-topics:
	yc managed-kafka topic list --cluster-id $$(yc managed-kafka cluster get otus-kafka --format json 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin)['id'])")

# SSH to MLflow VM
ssh-mlflow:
	ssh ubuntu@$(MLFLOW_HOST)

.PHONY: init plan apply destroy destroy-infra kubeconfig destroy-k8s \
	upload-dags upload-scripts upload-venv upload-all rebuild-venv \
	deploy urls airflow-vars kafka-status kafka-topics ssh-mlflow
