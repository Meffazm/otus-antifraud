# otus-antifraud

Система обнаружения мошеннических финансовых транзакций.
Учебный проект курса [MLOps (Otus)](https://otus.ru/).

## Архитектура

```
S3 (raw CSV) → Airflow DAG → DataProc (PySpark) → S3 (Parquet)
                    ↑                                    ↓
               расписание                         Feature Store (Feast)
                                                         ↓
                              MLFlow ← Model Training (PySpark) → S3 (artifacts)
```

## Структура репозитория

```
├── infra/                  # Terraform — инфраструктура Yandex Cloud
│   ├── main.tf             # SA, VPC, S3, Managed Airflow
│   ├── variables.tf        # Параметры
│   ├── outputs.tf          # Выходные данные (bucket, airflow URL)
│   └── Makefile            # apply, destroy, upload-all, airflow-vars
├── dags/
│   └── data_cleaning_dag.py  # Airflow DAG — автоочистка данных
├── scripts/
│   └── data_cleaning.py    # PySpark-скрипт очистки данных
├── notebooks/
│   ├── data_quality_analysis.ipynb  # Анализ качества данных
│   └── feast_features.ipynb         # Feast Feature Store демо
├── feature_store/          # Feast — Feature Views
│   ├── features.py         # 2 batch + 1 on-demand Feature View
│   └── data/               # Демо-данные
└── docs/                   # Проектная документация
```

## Инфраструктура

**Yandex Cloud** (Terraform):
- **S3** — хранение данных, DAG, скриптов
- **Managed Airflow** — оркестрация пайплайнов очистки
- **DataProc** — эфемерный Spark-кластер (создаётся/удаляется через DAG)
- **VPC** — сеть с NAT-шлюзом и security group

Подробнее: [infra/README.md](infra/README.md)

## Данные

~1.9 млрд транзакций (40 файлов × ~47 млн строк). Доля fraud: ~6.5%.

Обнаружено 6 типов проблем качества → автоматическая очистка → Parquet.

## Прогресс

| Этап | Статус | Ветка |
|------|--------|-------|
| Инфраструктура (Terraform, S3, DataProc) | ✅ | `infra-testing` |
| Анализ качества и очистка данных | ✅ | `data-drift` |
| Feature Store (Feast) | ✅ | `feature-store` |
| Автоматизация пайплайна (Airflow) | 🔄 | `autoclean` |
| Обучение модели + MLFlow | ⬜ | — |
| Онлайн-скоринг сервис | ⬜ | — |
| Мониторинг дрейфа | ⬜ | — |

## Документация

- [Цели и метрики](docs/01_goals_and_metrics.md)
- [MISSION Canvas](docs/02_mission_canvas.md)
- [Декомпозиция системы](docs/03_system_decomposition.md)
- [S.M.A.R.T. задачи (MVP)](docs/04_smart_tasks.md)
