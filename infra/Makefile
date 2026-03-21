init:
	terraform init -upgrade

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

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
