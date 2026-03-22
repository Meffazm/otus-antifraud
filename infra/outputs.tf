# S3 bucket name
output "bucket_name" {
  value = yandex_storage_bucket.data_bucket.bucket
}

# S3 endpoint for the bucket
output "bucket_endpoint" {
  value = "https://storage.yandexcloud.net/${yandex_storage_bucket.data_bucket.bucket}"
}

# Static access key ID
output "access_key" {
  value     = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  sensitive = true
}

# Static secret key
output "secret_key" {
  value     = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  sensitive = true
}

# Airflow web UI URL
output "airflow_url" {
  value = "https://c-${yandex_airflow_cluster.airflow.id}.airflow.yandexcloud.net"
}

# MLflow UI URL (external)
output "mlflow_url" {
  value = "http://${yandex_compute_instance.mlflow.network_interface[0].nat_ip_address}:5000"
}

# MLflow internal URL (for DataProc nodes)
output "mlflow_internal_url" {
  value = "http://${yandex_compute_instance.mlflow.network_interface[0].ip_address}:5000"
}

# Kafka bootstrap server
output "kafka_bootstrap" {
  value = [for h in yandex_mdb_kafka_cluster.kafka.host : "${h.name}:9091"][0]
}

# Container Registry ID
output "container_registry_id" {
  value = yandex_container_registry.registry.id
}

# K8s cluster ID
output "k8s_cluster_id" {
  value = yandex_kubernetes_cluster.k8s.id
}

# K8s cluster endpoint
output "k8s_cluster_endpoint" {
  value = yandex_kubernetes_cluster.k8s.master[0].external_v4_endpoint
}
