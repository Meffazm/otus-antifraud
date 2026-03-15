# ============================================================
# IAM: Service account and roles
# ============================================================

resource "yandex_iam_service_account" "sa" {
  name        = var.yc_service_account_name
  description = "Service account for DataProc cluster and S3 access"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_roles" {
  for_each = toset([
    "storage.admin",
    "dataproc.editor",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
  ])

  folder_id = var.yc_folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "Static access key for S3"
}

# ============================================================
# Network: VPC, subnet, NAT, security group
# ============================================================

resource "yandex_vpc_network" "network" {
  name = var.yc_network_name
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = var.yc_nat_gateway_name
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = var.yc_route_table_name
  network_id = yandex_vpc_network.network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_vpc_subnet" "subnet" {
  name           = var.yc_subnet_name
  zone           = var.yc_zone
  network_id     = yandex_vpc_network.network.id
  v4_cidr_blocks = [var.yc_subnet_range]
  route_table_id = yandex_vpc_route_table.route_table.id
}

resource "yandex_vpc_security_group" "security_group" {
  name        = var.yc_security_group_name
  description = "Security group for DataProc cluster"
  network_id  = yandex_vpc_network.network.id

  # Ingress: only SSH, Jupyter, and internal cluster traffic
  ingress {
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

  ingress {
    protocol       = "TCP"
    description    = "Jupyter Notebook"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 8888
  }

  ingress {
    protocol          = "ANY"
    description       = "Internal cluster traffic"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

  # Egress: allow all outgoing from master + internal cluster traffic
  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol          = "ANY"
    description       = "Internal cluster traffic"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}

# ============================================================
# Storage: S3 bucket
# ============================================================

resource "yandex_storage_bucket" "data_bucket" {
  bucket        = "${var.yc_bucket_name}-${var.yc_folder_id}"
  access_key    = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  force_destroy = true

  # Public read access
  anonymous_access_flags {
    read        = true
    list        = true
    config_read = true
  }
}

# ============================================================
# DataProc: Spark cluster
# ============================================================

resource "yandex_dataproc_cluster" "dataproc_cluster" {
  depends_on = [yandex_resourcemanager_folder_iam_member.sa_roles]

  name               = var.yc_dataproc_cluster_name
  description        = "Spark cluster for OTUS MLOps anti-fraud project"
  bucket             = yandex_storage_bucket.data_bucket.bucket
  service_account_id = yandex_iam_service_account.sa.id
  zone_id            = var.yc_zone
  security_group_ids = [yandex_vpc_security_group.security_group.id]

  labels = {
    created_by = "terraform"
  }

  cluster_config {
    version_id = var.yc_dataproc_version

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "HIVE", "TEZ"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      ssh_public_keys = [file(var.public_key_path)]
    }

    # Master subcluster: s3-c2-m8, 40 GB
    subcluster_spec {
      name = "master"
      role = "MASTERNODE"
      resources {
        resource_preset_id = var.dataproc_master_resources.resource_preset_id
        disk_type_id       = var.dataproc_master_resources.disk_type_id
        disk_size          = var.dataproc_master_resources.disk_size
      }
      subnet_id        = yandex_vpc_subnet.subnet.id
      hosts_count      = 1
      assign_public_ip = true
    }

    # Data subcluster: s3-c4-m16, 128 GB, 3 hosts
    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = var.dataproc_data_resources.resource_preset_id
        disk_type_id       = var.dataproc_data_resources.disk_type_id
        disk_size          = var.dataproc_data_resources.disk_size
      }
      subnet_id   = yandex_vpc_subnet.subnet.id
      hosts_count = var.dataproc_data_resources.hosts_count
    }
  }
}
