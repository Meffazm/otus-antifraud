# Облачная инфраструктура для проекта Anti-Fraud

## Описание

Terraform-конфигурация для развёртывания инфраструктуры в Yandex Cloud:
- S3-бакет для хранения данных о транзакциях
- VPC-сеть с NAT-шлюзом и группой безопасности
- Spark-кластер Yandex Data Processing (HDFS, YARN, SPARK, HIVE, TEZ)

## Состав кластера

| Подкластер | Класс хоста | Кол-во хостов | Диск |
|------------|-------------|---------------|------|
| Master     | s3-c2-m8    | 1             | 40 ГБ (network-ssd) |
| Data       | s3-c4-m16   | 3             | 128 ГБ (network-ssd) |

## Быстрый старт

```bash
cp terraform.tfvars.example terraform.tfvars
vi terraform.tfvars # заполнить yc_token, yc_cloud_id, yc_folder_id

make init
make plan
make apply
```

## Точка доступа к бакету

https://storage.yandexcloud.net/otus-mlops-data-b1g7vl7oovirupu7q3vf

## Анализ качества данных

Проведён анализ датасета транзакций. Обнаружены следующие проблемы:

| # | Тип проблемы | Колонка | Описание | ~Кол-во на файл |
|---|-------------|---------|----------|-----------------|
| 1 | Missing values | terminal_id | NULL значения | 85 |
| 2 | Invalid values | terminal_id | Значение 'Err' вместо числового ID | 2 213 |
| 3 | Out-of-range | customer_id | Отрицательные значения ID | 96 |
| 4 | Invalid format | tx_datetime | Некорректное время '24:00:00' | 92 |
| 5 | Zero amounts | tx_amount | Нулевые суммы транзакций | 936 |
| 6 | Duplicates | tranaction_id | Повторяющиеся ID транзакций | 16 |

Подробный анализ — в ноутбуке `notebooks/data_quality_analysis.ipynb`.

## Очистка данных

Скрипт очистки: `scripts/data_cleaning.py`

Запуск на кластере:
```bash
make upload
make clean-data
```

Очищенные данные сохраняются в формате Parquet в `s3://BUCKET_NAME/cleaned/`.

## Оценка затрат

Цена кластера Data Proc — 29 561,93 руб./мес.
- Intel Ice Lake 100% 2vCPU 8Gb RAM 40Gb SSD (master)
- 3x Intel Ice Lake 100% 4vCPU 16Gb RAM 128Gb SSD (workers)

Цена S3-бакета — 327,72 руб./мес.
- Стандартный Object Storage 128Gb, 100 000 GET/POST-операций

Хранение данных в S3 ~в 90 раз дешевле HDFS.

## Удаление

```bash
make destroy
```
