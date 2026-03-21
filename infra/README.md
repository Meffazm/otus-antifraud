# Облачная инфраструктура для проекта Anti-Fraud

## Описание

Terraform-конфигурация для развёртывания инфраструктуры в Yandex Cloud:
- S3-бакет для хранения данных, DAG, скриптов, артефактов MLflow
- VPC-сеть с NAT-шлюзом и группой безопасности
- Managed Apache Airflow (оркестрация пайплайнов)
- Managed PostgreSQL (backend store для MLflow)
- VM с MLflow Tracking Server (порт 5000)
- DataProc кластер — создаётся/удаляется автоматически через Airflow DAG

## Быстрый старт

```bash
cp terraform.tfvars.example terraform.tfvars
# Заполнить: yc_token, yc_cloud_id, yc_folder_id, airflow_admin_password, mlflow_db_password

make init
make apply

# Загрузить DAG и скрипты в S3
make upload-all

# Показать URL Airflow и MLflow
make urls

# Импортировать переменные в Airflow UI
make airflow-vars
```

## URL сервисов

```bash
make urls
```

- **Airflow UI** — `https://c-<ID>.airflow.yandexcloud.net` (admin / airflow_admin_password)
- **MLflow UI** — `http://<MLflow_IP>:5000`

## Airflow DAGs

| DAG | Расписание | Описание |
|-----|-----------|----------|
| `data_cleaning` | daily | Очистка данных: DataProc → spark-submit → Parquet |
| `model_training` | weekly | Обучение модели: DataProc → spark-submit → MLflow |

## Удаление

```bash
# Удалить Airflow, MLflow, PostgreSQL, сеть; оставить S3 и SA
make destroy-infra

# Удалить всё, включая S3
make destroy
```
