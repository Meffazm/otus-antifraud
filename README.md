# otus-antifraud

Система обнаружения мошеннических финансовых транзакций.
Учебный проект курса [MLOps (Otus)](https://otus.ru/).

## Архитектура

```
S3 (raw CSV) → Data Cleaning (PySpark) → S3 (Parquet) → Feature Store (Feast)
                                                              ↓
                              MLFlow ← Model Training (PySpark/Spark) → S3 (artifacts)
                                              ↓
                              Airflow (оркестрация пайплайнов)
```

## Структура репозитория

```
├── infra/                  # Terraform — инфраструктура Yandex Cloud
│   ├── main.tf             # SA, VPC, S3, DataProc кластер
│   ├── variables.tf        # Параметры (кластер, сеть, бакет)
│   ├── outputs.tf          # Выходные данные (bucket, ключи, cluster ID)
│   └── Makefile            # apply, destroy, ssh, jupyter, clean-data
├── scripts/
│   └── data_cleaning.py    # PySpark-скрипт очистки данных
├── notebooks/
│   └── data_quality_analysis.ipynb  # Анализ качества данных
└── docs/                   # Проектная документация
    ├── 01_goals_and_metrics.md
    ├── 02_mission_canvas.md
    ├── 03_system_decomposition.md
    └── 04_smart_tasks.md
```

## Инфраструктура

**Yandex Cloud** (Terraform):
- **S3** — хранение сырых данных и очищенных Parquet-файлов
- **DataProc** — Spark-кластер (master s3-c2-m8 + 3× data s3-c4-m16)
- **VPC** — сеть с NAT-шлюзом и security group

Подробнее: [infra/README.md](infra/README.md)

## Данные

~1.9 млрд транзакций (40 файлов × ~47 млн строк). Доля fraud: ~6.5%.

Обнаружено 6 типов проблем качества → автоматическая очистка → Parquet.

## Прогресс

| Этап | Статус | Ветка |
|------|--------|-------|
| Инфраструктура (Terraform, S3, DataProc) | ✅ | `infra-testing` |
| Анализ качества и очистка данных | 🔄 | `data-drift` |
| Feature Store (Feast) | ⬜ | — |
| Автоматизация пайплайна (Airflow) | ⬜ | — |
| Обучение модели + MLFlow | ⬜ | — |
| Онлайн-скоринг сервис | ⬜ | — |
| Мониторинг дрейфа | ⬜ | — |

## Документация

- [Цели и метрики](docs/01_goals_and_metrics.md)
- [MISSION Canvas](docs/02_mission_canvas.md)
- [Декомпозиция системы](docs/03_system_decomposition.md)
- [S.M.A.R.T. задачи (MVP)](docs/04_smart_tasks.md)
