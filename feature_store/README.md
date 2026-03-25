# Feature Store (Feast)

## Что сделано

Создан локальный Feature Store на базе Feast с тремя Feature Views:

| Feature View | Тип | Признаки |
|---|---|---|
| `driver_efficiency` | Batch | conv_rate, acc_rate |
| `driver_activity` | Batch | avg_daily_trips |
| `driver_scoring` | On-demand | weighted_rating, trip_score, surge_adjusted_score |

On-demand view `driver_scoring` вычисляет композитный скоринг водителя в реальном времени, принимая `surge_multiplier` из запроса.

## Как запустить

```bash
uv venv .venv-feast --python 3.11
source .venv-feast/bin/activate
uv pip install feast pandas jupyter numpy

# Применить определения фичей
cd feature_store
feast apply

# Открыть ноутбук
jupyter notebook ../notebooks/feast_features.ipynb
```

## Структура

```
feature_store/
├── feature_store.yaml   # Конфигурация Feast
├── features.py          # Определения Feature Views
└── data/
    └── driver_stats.parquet  # Демо-данные

notebooks/
└── feast_features.ipynb     # Демонстрация запросов к Feature Store
```

## Ноутбук

`notebooks/feast_features.ipynb` демонстрирует:
- Получение исторических фичей для обучения (`get_historical_features`)
- Получение онлайн-фичей для инференса (`get_online_features`)
- Работу on-demand трансформаций с параметром из запроса
