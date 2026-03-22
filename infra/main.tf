# ============================================================
# IAM: Service account and roles
# ============================================================

resource "yandex_iam_service_account" "sa" {
  name        = var.yc_service_account_name
  description = "Service account for DataProc, Airflow, and S3"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_roles" {
  for_each = toset([
    "storage.admin",
    "dataproc.editor",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
    "compute.admin",
    "managed-airflow.integrationProvider",
    "managed-kafka.admin",
    "container-registry.images.puller",
    "container-registry.images.pusher",
    "k8s.admin",
    "k8s.clusters.agent",
    "k8s.tunnelClusters.agent",
    "vpc.publicAdmin",
    "load-balancer.admin",
  ])

  folder_id = var.yc_folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

# Static key for S3 access
resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "Static access key for S3"
}

# Authorized key for DataProc (needed by Airflow to create clusters)
resource "yandex_iam_service_account_key" "sa_auth_key" {
  service_account_id = yandex_iam_service_account.sa.id
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
  description = "Security group for DataProc and Airflow"
  network_id  = yandex_vpc_network.network.id

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
    protocol       = "TCP"
    description    = "MLflow UI"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 5000
  }

  ingress {
    protocol       = "TCP"
    description    = "Kafka SASL_SSL"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 9091
  }

  ingress {
    protocol       = "TCP"
    description    = "HTTP (K8s LoadBalancer)"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 80
  }

  ingress {
    protocol       = "TCP"
    description    = "K8s NLB health checks"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 10256
  }

  ingress {
    protocol       = "TCP"
    description    = "K8s API server"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 6443
  }

  ingress {
    protocol       = "TCP"
    description    = "K8s API server (alt)"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 443
  }

  ingress {
    protocol       = "TCP"
    description    = "K8s NodePort range"
    v4_cidr_blocks = ["0.0.0.0/0"]
    from_port      = 30000
    to_port        = 32767
  }

  ingress {
    protocol          = "ANY"
    description       = "Internal cluster traffic"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }

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
# Storage: S3 bucket (data + DAGs)
# ============================================================

resource "yandex_storage_bucket" "data_bucket" {
  bucket        = "${var.yc_bucket_name}-${var.yc_folder_id}"
  access_key    = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  force_destroy = true

  anonymous_access_flags {
    read        = true
    list        = true
    config_read = true
  }
}

# ============================================================
# Airflow: Managed Service
# ============================================================

resource "yandex_airflow_cluster" "airflow" {
  depends_on = [yandex_resourcemanager_folder_iam_member.sa_roles]

  name               = var.yc_airflow_cluster_name
  service_account_id = yandex_iam_service_account.sa.id
  subnet_ids         = [yandex_vpc_subnet.subnet.id]
  admin_password     = var.airflow_admin_password

  code_sync = {
    s3 = {
      bucket = yandex_storage_bucket.data_bucket.bucket
    }
  }

  webserver = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  scheduler = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  worker = {
    min_count          = 1
    max_count          = 2
    resource_preset_id = "c1-m4"
  }

  airflow_config = {
    "api" = {
      "auth_backends" = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    }
    "scheduler" = {
      "dag_dir_list_interval" = "30"
    }
  }

  logging = {
    enabled   = true
    folder_id = var.yc_folder_id
    min_level = "INFO"
  }
}

# ============================================================
# MLflow: Managed PostgreSQL + VM with MLflow server
# ============================================================

resource "yandex_mdb_postgresql_cluster" "mlflow_db" {
  name        = "otus-mlflow-db"
  environment = "PRESTABLE"
  network_id  = yandex_vpc_network.network.id

  config {
    version = 15
    resources {
      resource_preset_id = "s2.micro"
      disk_type_id       = "network-ssd"
      disk_size          = 10
    }
  }

  database {
    name  = "mlflow"
    owner = "mlflow"
  }

  user {
    name     = "mlflow"
    password = var.mlflow_db_password
    permission {
      database_name = "mlflow"
    }
  }

  host {
    zone      = var.yc_zone
    subnet_id = yandex_vpc_subnet.subnet.id
  }
}

resource "yandex_compute_instance" "mlflow" {
  name        = var.mlflow_vm_name
  platform_id = "standard-v3"
  zone        = var.yc_zone

  resources {
    cores  = 2
    memory = 4
  }

  scheduling_policy {
    preemptible = true
  }

  boot_disk {
    initialize_params {
      image_id = "fd81radk00nmm2jpqh94"  # Ubuntu 22.04 LTS
      size     = 15
    }
  }

  network_interface {
    subnet_id          = yandex_vpc_subnet.subnet.id
    nat                = true
    security_group_ids = [yandex_vpc_security_group.security_group.id]
  }

  metadata = {
    ssh-keys  = "ubuntu:${file(var.public_key_path)}"
    user-data = templatefile("${path.module}/scripts/mlflow_setup.sh", {
      s3_access_key  = yandex_iam_service_account_static_access_key.sa_static_key.access_key
      s3_secret_key  = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
      s3_bucket_name = yandex_storage_bucket.data_bucket.bucket
      db_host        = yandex_mdb_postgresql_cluster.mlflow_db.host[0].fqdn
      db_password    = var.mlflow_db_password
    })
  }

  depends_on = [yandex_mdb_postgresql_cluster.mlflow_db]
}

# ============================================================
# Kafka: Managed Service
# ============================================================

resource "yandex_mdb_kafka_cluster" "kafka" {
  name        = "otus-kafka"
  environment = "PRODUCTION"
  network_id  = yandex_vpc_network.network.id
  subnet_ids  = [yandex_vpc_subnet.subnet.id]
  security_group_ids = [yandex_vpc_security_group.security_group.id]

  config {
    version          = "3.7"
    brokers_count    = 1
    zones            = [var.yc_zone]
    assign_public_ip = true

    kafka {
      resources {
        resource_preset_id = "s2.micro"
        disk_type_id       = "network-ssd"
        disk_size          = 32
      }
      kafka_config {
        auto_create_topics_enable = true
      }
    }
  }

  topic {
    name               = "transactions"
    partitions         = 3
    replication_factor = 1
  }

  topic {
    name               = "predictions"
    partitions         = 3
    replication_factor = 1
  }

  user {
    name     = "producer"
    password = var.kafka_password
    permission {
      topic_name = "transactions"
      role       = "ACCESS_ROLE_PRODUCER"
    }
  }

  user {
    name     = "consumer"
    password = var.kafka_password
    permission {
      topic_name = "transactions"
      role       = "ACCESS_ROLE_CONSUMER"
    }
    permission {
      topic_name = "predictions"
      role       = "ACCESS_ROLE_PRODUCER"
    }
    permission {
      topic_name = "predictions"
      role       = "ACCESS_ROLE_CONSUMER"
    }
  }
}

# ============================================================
# Output: Airflow variables JSON (for import into Airflow UI)
# ============================================================

resource "local_file" "airflow_variables" {
  content = jsonencode({
    YC_ZONE              = var.yc_zone
    YC_FOLDER_ID         = var.yc_folder_id
    YC_SUBNET_ID         = yandex_vpc_subnet.subnet.id
    YC_SSH_PUBLIC_KEY    = trimspace(file(var.public_key_path))
    S3_ENDPOINT_URL      = "https://storage.yandexcloud.net"
    S3_ACCESS_KEY        = yandex_iam_service_account_static_access_key.sa_static_key.access_key
    S3_SECRET_KEY        = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
    S3_BUCKET_NAME       = yandex_storage_bucket.data_bucket.bucket
    DP_SECURITY_GROUP_ID = yandex_vpc_security_group.security_group.id
    DP_SA_ID             = yandex_iam_service_account.sa.id
    DP_SA_AUTH_KEY_PUBLIC_KEY = yandex_iam_service_account_key.sa_auth_key.public_key
    MLFLOW_TRACKING_URI  = "http://${yandex_compute_instance.mlflow.network_interface[0].ip_address}:5000"
    MLFLOW_EXTERNAL_URL  = "http://${yandex_compute_instance.mlflow.network_interface[0].nat_ip_address}:5000"
    KAFKA_BOOTSTRAP      = "${[for h in yandex_mdb_kafka_cluster.kafka.host : "${h.name}:9091"][0]}"
    KAFKA_PASSWORD       = var.kafka_password
    DP_SA_JSON = jsonencode({
      id                 = yandex_iam_service_account_key.sa_auth_key.id
      service_account_id = yandex_iam_service_account.sa.id
      created_at         = yandex_iam_service_account_key.sa_auth_key.created_at
      public_key         = yandex_iam_service_account_key.sa_auth_key.public_key
      private_key        = yandex_iam_service_account_key.sa_auth_key.private_key
    })
  })
  filename        = "${path.module}/airflow_variables.json"
  file_permission = "0600"
}

# ============================================================
# Container Registry
# ============================================================

resource "yandex_container_registry" "registry" {
  name      = "otus-antifraud-registry"
  folder_id = var.yc_folder_id
}

# ============================================================
# Kubernetes: Managed cluster and node group
# ============================================================

resource "yandex_kubernetes_cluster" "k8s" {
  name        = "otus-antifraud-k8s"
  network_id  = yandex_vpc_network.network.id

  master {
    version = "1.34"
    zonal {
      zone      = var.yc_zone
      subnet_id = yandex_vpc_subnet.subnet.id
    }
    public_ip = true

    security_group_ids = [yandex_vpc_security_group.security_group.id]
  }

  service_account_id      = yandex_iam_service_account.sa.id
  node_service_account_id = yandex_iam_service_account.sa.id

  release_channel         = "RAPID"

  cluster_ipv4_range = "10.200.0.0/16"
  service_ipv4_range = "10.201.0.0/16"

  depends_on = [yandex_resourcemanager_folder_iam_member.sa_roles]
}

resource "yandex_kubernetes_node_group" "k8s_nodes" {
  cluster_id = yandex_kubernetes_cluster.k8s.id
  name       = "otus-antifraud-nodes"
  version    = "1.34"

  instance_template {
    platform_id = "standard-v3"

    resources {
      cores  = 2
      memory = 4
    }

    boot_disk {
      size = 50
      type = "network-ssd"
    }

    network_interface {
      subnet_ids         = [yandex_vpc_subnet.subnet.id]
      nat                = true
      security_group_ids = [yandex_vpc_security_group.security_group.id]
    }

    scheduling_policy {
      preemptible = true
    }
  }

  scale_policy {
    fixed_scale {
      size = 3
    }
  }

  allocation_policy {
    location {
      zone = var.yc_zone
    }
  }
}
