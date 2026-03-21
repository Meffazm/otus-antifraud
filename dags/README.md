# Автоматизация очистки данных (Airflow)

## Что сделано

DAG `data_cleaning` для ежедневной автоматической очистки новых данных:

1. Проверяет наличие необработанных `.txt` файлов в S3-бакете
2. Создаёт временный Spark-кластер (DataProc)
3. Запускает `data_cleaning.py` через `spark-submit` на всех файлах
4. Обновляет манифест обработанных файлов
5. Удаляет кластер

## Как запустить

```bash
cd infra

# Добавить airflow_admin_password в terraform.tfvars
make init
make apply

# Загрузить DAG и скрипт очистки в S3
make upload-all

# Импортировать переменные в Airflow UI
make airflow-vars
```

В Airflow UI (`terraform output airflow_url`):
1. Admin → Variables → Import Variables → загрузить `infra/airflow_variables.json`
2. DAG `data_cleaning` появится автоматически после синхронизации с S3

## Структура

```
dags/
└── data_cleaning_dag.py    # Airflow DAG

scripts/
└── data_cleaning.py        # PySpark-скрипт очистки (загружается в s3://BUCKET/src/)
```

## Инфраструктура

- **Airflow**: Yandex Cloud Managed Service (Terraform)
- **DataProc**: эфемерный кластер, создаётся/удаляется каждый запуск DAG
- **S3**: хранение DAG, скриптов, данных, манифеста обработанных файлов
