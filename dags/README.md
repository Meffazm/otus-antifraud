# Airflow DAGs

## data_cleaning

Ежедневная очистка данных транзакций.

1. Проверяет наличие необработанных файлов в S3
2. Создаёт временный Spark-кластер (DataProc)
3. Запускает `data_cleaning.py` через spark-submit
4. Обновляет манифест обработанных файлов
5. Удаляет кластер

## model_training

Еженедельное переобучение и валидация модели антифрода.

1. Создаёт временный Spark-кластер (DataProc)
2. Обучает модель (`train_model.py`) — LogisticRegression, логирует в MLflow
3. Валидирует модель (`validate_model.py`) — A/B тест с bootstrap + t-test
4. Если улучшение статистически значимо — промотирует модель в champion
5. Удаляет кластер

Валидация:
- Bootstrap resampling (50 итераций) на тестовых данных
- t-test для сравнения champion vs candidate
- Cohen's d для оценки размера эффекта
- Деплой только при p < α и improvement > 0

## Как запустить

```bash
cd infra
make upload-all
make airflow-vars
```

> При повторном импорте переменных: сначала удалите старые, затем импортируйте заново.
