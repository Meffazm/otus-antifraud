# Airflow DAGs

## data_cleaning

Ежедневная очистка данных транзакций.

1. Проверяет наличие необработанных файлов в S3
2. Создаёт временный Spark-кластер (DataProc)
3. Запускает `data_cleaning.py` через spark-submit
4. Обновляет манифест обработанных файлов
5. Удаляет кластер

## model_training

Еженедельное переобучение модели антифрода.

1. Создаёт временный Spark-кластер (DataProc)
2. Запускает `train_model.py` — обучение LogisticRegression на очищенных данных
3. Логирует метрики (AUC, F1, Accuracy) и модель в MLflow
4. Регистрирует модель в MLflow Model Registry
5. Удаляет кластер

Для работы `train_model.py` используется Python venv (с mlflow), который распространяется на DataProc через `spark.yarn.dist.archives`.

## Как запустить

```bash
cd infra
make upload-all     # загрузить DAGs, скрипты в S3
make airflow-vars   # показать инструкцию по импорту переменных
```

> **Важно:** При повторном импорте переменных в Airflow, сначала удалите старые (Select all → Delete), затем импортируйте заново.
