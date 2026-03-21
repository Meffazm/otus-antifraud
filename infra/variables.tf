# Yandex Cloud credentials
variable "yc_token" {
  type        = string
  description = "Yandex Cloud OAuth token"
  sensitive   = true
}

variable "yc_cloud_id" {
  type        = string
  description = "Yandex Cloud ID"
}

variable "yc_folder_id" {
  type        = string
  description = "Yandex Cloud Folder ID"
}

variable "yc_zone" {
  type        = string
  description = "Availability zone for resources"
  default     = "ru-central1-a"
}

# Network
variable "yc_network_name" {
  type        = string
  description = "Name of the VPC network"
  default     = "otus-network"
}

variable "yc_subnet_name" {
  type        = string
  description = "Name of the subnet"
  default     = "otus-subnet"
}

variable "yc_subnet_range" {
  type        = string
  description = "CIDR block for the subnet"
  default     = "10.0.0.0/24"
}

variable "yc_nat_gateway_name" {
  type        = string
  description = "Name of the NAT gateway"
  default     = "otus-nat-gateway"
}

variable "yc_route_table_name" {
  type        = string
  description = "Name of the route table"
  default     = "otus-route-table"
}

variable "yc_security_group_name" {
  type        = string
  description = "Name of the security group"
  default     = "otus-security-group"
}

# Service account
variable "yc_service_account_name" {
  type        = string
  description = "Name of the service account"
  default     = "otus-sa"
}

# S3 bucket
variable "yc_bucket_name" {
  type        = string
  description = "Name prefix for the S3 bucket"
  default     = "otus-mlops-data"
}

# SSH
variable "public_key_path" {
  type        = string
  description = "Path to the SSH public key"
  default     = "~/.ssh/id_ed25519.pub"
}

# Airflow
variable "yc_airflow_cluster_name" {
  type        = string
  description = "Name of the Managed Airflow cluster"
  default     = "otus-airflow"
}

variable "airflow_admin_password" {
  type        = string
  description = "Admin password for Airflow web UI (min 8 chars)"
  sensitive   = true
}

# MLflow VM
variable "mlflow_vm_name" {
  type        = string
  description = "Name of the MLflow VM"
  default     = "otus-mlflow"
}

variable "mlflow_db_password" {
  type        = string
  description = "Password for MLflow PostgreSQL database"
  sensitive   = true
}
