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

# DataProc cluster
variable "yc_dataproc_cluster_name" {
  type        = string
  description = "Name of the DataProc cluster"
  default     = "otus-dataproc-cluster"
}

variable "yc_dataproc_version" {
  type        = string
  description = "DataProc image version"
  default     = "2.0"
}

variable "dataproc_master_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
  })
  description = "Master subcluster resource configuration"
  default = {
    resource_preset_id = "s3-c2-m8"
    disk_type_id       = "network-ssd"
    disk_size          = 40
  }
}

variable "dataproc_data_resources" {
  type = object({
    resource_preset_id = string
    disk_type_id       = string
    disk_size          = number
    hosts_count        = number
  })
  description = "Data subcluster resource configuration"
  default = {
    resource_preset_id = "s3-c4-m16"
    disk_type_id       = "network-ssd"
    disk_size          = 128
    hosts_count        = 3
  }
}
