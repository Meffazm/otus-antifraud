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
REGISTRY_ID = $(shell terraform output -raw container_registry_id 2>/dev/null)
K8S_CLUSTER_ID = $(shell terraform output -raw k8s_cluster_id 2>/dev/null)
API_IMAGE = cr.yandex/$(REGISTRY_ID)/antifraud-api

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

# Build and push Docker image to YC Container Registry
docker-build:
	cd .. && docker build --platform linux/amd64 -t $(API_IMAGE):latest .

docker-push: docker-build
	docker push $(API_IMAGE):latest

# Deploy to K8s: build, push, apply manifests
k8s-deploy: docker-push kubeconfig
	cd .. && sed "s|cr.yandex/REGISTRY_ID/antifraud-api:latest|$(API_IMAGE):latest|g" k8s/deployment.yaml | kubectl apply -f -
	kubectl apply -f ../k8s/service.yaml
	kubectl rollout status deployment/antifraud-api --timeout=120s

# Show K8s service external IP
k8s-status:
	@kubectl get pods -l app=antifraud-api
	@echo ""
	@kubectl get svc antifraud-api

# Test the deployed API
k8s-test:
	@EXTERNAL_IP=$$(kubectl get svc antifraud-api -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	if [ -z "$$EXTERNAL_IP" ]; then echo "No external IP yet. Run 'make k8s-status' to check."; exit 1; fi; \
	echo "Health check:"; \
	curl -s http://$$EXTERNAL_IP/health; echo ""; \
	echo ""; \
	echo "Predict:"; \
	curl -s -X POST http://$$EXTERNAL_IP/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id": 12345, "tx_amount": 150.0, "tx_datetime": "2026-03-22 14:30:00"}'; echo ""

# Convert PySpark model from S3 to JSON
convert-model:
	cd .. && python3 scripts/convert_model.py \
		--bucket $(BUCKET_NAME) \
		--model-path models/model_20260321 \
		--output model/model.json \
		--access-key $$(terraform -chdir=infra output -raw access_key) \
		--secret-key $$(terraform -chdir=infra output -raw secret_key)

# Print GitHub Secrets values for easy copy-paste
github-secrets:
	@echo "Configure these in GitHub → Settings → Secrets → Actions:"
	@echo ""
	@echo "YC_FOLDER_ID     = $(shell yc config get folder-id 2>/dev/null)"
	@echo "YC_REGISTRY_ID   = $(REGISTRY_ID)"
	@echo "YC_K8S_CLUSTER_ID = $(K8S_CLUSTER_ID)"
	@echo ""
	@echo "YC_SA_KEY: run this and paste the output:"
	@echo "  yc iam key create --service-account-name otus-sa --output /dev/stdout"

# Show all service URLs
urls:
	@echo "Airflow URL:  $(shell terraform output -raw airflow_url 2>/dev/null)"
	@echo "MLflow URL:   $(shell terraform output -raw mlflow_url 2>/dev/null)"
	@echo "Kafka:        $(KAFKA_BOOTSTRAP)"
	@echo "K8s endpoint: $(shell terraform output -raw k8s_cluster_endpoint 2>/dev/null)"
	@echo "Registry ID:  $(REGISTRY_ID)"

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

# Install Prometheus + Grafana via Helm
helm-monitoring:
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
		--set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
		--set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false \
		--wait --timeout 5m
	kubectl apply -f ../k8s/servicemonitor.yaml
	kubectl apply -f ../k8s/prometheusrule.yaml

# Install Airflow in K8s via Helm (syncs DAGs from git)
helm-airflow:
	helm repo add apache-airflow https://airflow.apache.org
	helm repo update
	helm upgrade --install airflow apache-airflow/airflow \
		--set dags.gitSync.enabled=true \
		--set dags.gitSync.repo=https://github.com/Meffazm/otus-antifraud-dags.git \
		--set dags.gitSync.branch=main \
		--set dags.gitSync.subPath=dags \
		--set dags.gitSync.wait=60 \
		--wait --timeout 10m

# Apply HPA (autoscaling 4-6 replicas)
k8s-hpa:
	kubectl apply -f ../k8s/hpa.yaml
	kubectl get hpa antifraud-api

# Port-forward Grafana (http://localhost:3000, admin/prom-operator)
grafana:
	@echo "Grafana: http://localhost:3000  (admin / prom-operator)"
	kubectl port-forward svc/monitoring-grafana 3000:80

# Port-forward Prometheus (http://localhost:9090)
prometheus:
	@echo "Prometheus: http://localhost:9090"
	kubectl port-forward svc/monitoring-kube-prometheus-prometheus 9090:9090

# Port-forward Airflow (http://localhost:8080, admin/admin)
airflow-ui:
	@echo "Airflow: http://localhost:8080  (admin / admin)"
	kubectl port-forward svc/airflow-api-server 8080:8080

# Run load test against the deployed API
k8s-load-test:
	@EXTERNAL_IP=$$(kubectl get svc antifraud-api -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null); \
	if [ -z "$$EXTERNAL_IP" ]; then echo "No external IP yet."; exit 1; fi; \
	python3 ../scripts/load_test.py --url http://$$EXTERNAL_IP/predict --max-workers 50 --duration 600 --ramp-interval 30

.PHONY: init plan apply destroy destroy-infra kubeconfig destroy-k8s \
	upload-dags upload-scripts upload-venv upload-all rebuild-venv \
	deploy urls airflow-vars kafka-status kafka-topics ssh-mlflow \
	docker-build docker-push k8s-deploy k8s-status k8s-test convert-model github-secrets \
	helm-monitoring helm-airflow k8s-hpa grafana prometheus airflow-ui k8s-load-test
