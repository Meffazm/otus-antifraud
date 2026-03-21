# Облачная инфраструктура для проекта Anti-Fraud

## Описание

Terraform-конфигурация для развёртывания инфраструктуры в Yandex Cloud:
- S3-бакет для хранения данных, DAG и скриптов
- VPC-сеть с NAT-шлюзом и группой безопасности
- Managed Apache Airflow (оркестрация пайплайнов)
- DataProc кластер — создаётся/удаляется автоматически через Airflow DAG

## Быстрый старт

```bash
cp terraform.tfvars.example terraform.tfvars
# Заполнить: yc_token, yc_cloud_id, yc_folder_id, airflow_admin_password

make init
make apply

# Загрузить DAG и скрипты в S3
make upload-all

# Импортировать переменные в Airflow UI
make airflow-vars
```

## Точка доступа к бакету

https://storage.yandexcloud.net/otus-mlops-data-b1g7vl7oovirupu7q3vf

## Airflow DAG

DAG `data_cleaning` — ежедневный автоматический запуск очистки данных:

1. Проверка наличия новых `.txt` файлов в бакете
2. Создание эфемерного Spark-кластера (DataProc)
3. Запуск `data_cleaning.py` через spark-submit
4. Обновление манифеста обработанных файлов
5. Удаление кластера

> TODO: скриншот 3 успешных запусков

## Анализ качества данных

| # | Тип проблемы | Колонка | Описание | ~Кол-во/файл |
|---|-------------|---------|----------|-------------|
| 1 | Missing values | terminal_id | NULL значения | 85 |
| 2 | Invalid values | terminal_id | Значение 'Err' | 2 213 |
| 3 | Out-of-range | customer_id | Отрицательные ID | 96 |
| 4 | Invalid format | tx_datetime | Время '24:00:00' | 92 |
| 5 | Zero amounts | tx_amount | Нулевые суммы | 936 |
| 6 | Duplicates | tranaction_id | Повторяющиеся ID | 16 |

## Оценка затрат

- **Managed Airflow** — фиксированная стоимость (webserver + scheduler + workers)
- **DataProc** — эфемерный, оплата только за время работы (~1-2 ч/запуск)
- **S3** — ~327,72 руб./мес. за хранение данных

## Удаление

```bash
make destroy
```
