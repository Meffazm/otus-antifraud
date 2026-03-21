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

# DataProc cluster ID
output "dataproc_cluster_id" {
  value = yandex_dataproc_cluster.dataproc_cluster.id
}
