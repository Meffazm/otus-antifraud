init:
	terraform init -upgrade

plan:
	terraform plan

apply:
	terraform apply -auto-approve

destroy:
	terraform destroy -auto-approve

destroy-cluster:
	terraform destroy -auto-approve \
		-target=yandex_dataproc_cluster.dataproc_cluster \
		-target=yandex_vpc_security_group.security_group \
		-target=yandex_vpc_subnet.subnet \
		-target=yandex_vpc_route_table.route_table \
		-target=yandex_vpc_gateway.nat_gateway \
		-target=yandex_vpc_network.network

# Resolve master FQDN from DataProc cluster
CLUSTER_ID = $(shell terraform output -raw dataproc_cluster_id 2>/dev/null)
MASTER_FQDN = $(shell yc dataproc cluster list-hosts --id $(CLUSTER_ID) 2>/dev/null | grep MASTERNODE | awk '{print $$2}')
BUCKET_NAME = $(shell terraform output -raw bucket_name 2>/dev/null)

ssh:
	ssh ubuntu@$(MASTER_FQDN)

jupyter:
	ssh ubuntu@$(MASTER_FQDN) "\
		sudo pip3 install jupyter 2>/dev/null; \
		export SPARK_HOME=/usr/lib/spark; \
		export PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9-src.zip; \
		export PYSPARK_PYTHON=python3; \
		nohup jupyter notebook --ip=0.0.0.0 --no-browser > /tmp/jupyter.log 2>&1 & \
		sleep 3; cat /tmp/jupyter.log"

upload:
	scp ../notebooks/data_quality_analysis.ipynb ubuntu@$(MASTER_FQDN):/home/ubuntu/
	scp ../scripts/data_cleaning.py ubuntu@$(MASTER_FQDN):/home/ubuntu/

clean-data:
	ssh ubuntu@$(MASTER_FQDN) "spark-submit /home/ubuntu/data_cleaning.py \
		's3a://$(BUCKET_NAME)/*.txt' \
		's3a://$(BUCKET_NAME)/cleaned/'"
