init:
	terraform init -upgrade

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

# Destroy Airflow and network, keep S3 bucket and SA
destroy-infra:
	terraform destroy -auto-approve \
		-target=yandex_airflow_cluster.airflow \
		-target=yandex_vpc_security_group.security_group \
		-target=yandex_vpc_subnet.subnet \
		-target=yandex_vpc_route_table.route_table \
		-target=yandex_vpc_gateway.nat_gateway \
		-target=yandex_vpc_network.network \
		-target=yandex_iam_service_account_key.sa_auth_key \
		-target=local_file.airflow_variables

BUCKET_NAME = $(shell terraform output -raw bucket_name 2>/dev/null)

# Upload DAGs and scripts to S3 bucket (Airflow syncs from there)
upload-dags:
	s3cmd put --recursive ../dags/ s3://$(BUCKET_NAME)/dags/

upload-scripts:
	s3cmd put ../scripts/data_cleaning.py s3://$(BUCKET_NAME)/src/data_cleaning.py

upload-all: upload-dags upload-scripts

# Import Airflow variables from generated JSON
airflow-vars:
	@echo "Import airflow_variables.json into Airflow UI:"
	@echo "  Admin -> Variables -> Import Variables -> select airflow_variables.json"
	@echo ""
	@echo "Airflow URL: $(shell terraform output -raw airflow_url 2>/dev/null)"
