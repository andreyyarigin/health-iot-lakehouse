# Архитектура

> Детальное описание архитектуры и проектных решений платформы health-iot-lakehouse.

---

## 1. Общий обзор

Платформа следует архитектуре **lakehouse**: единое объектное хранилище (MinIO)
обслуживает как аналитические запросы (через Trino SQL), так и ML-нагрузки
(через прямое чтение Parquet через PyArrow). Apache Iceberg предоставляет слой
управления таблицами, делающий это возможным — ACID-транзакции, эволюция схемы
и time travel поверх Parquet-файлов.

Моделирование данных следует методологии **Data Vault 2.0**, разделяющей структуру
(хабы и линки) от контекста (спутники). Это даёт:

- Загрузку только добавлением — спутники накапливают историю, ничего не перезаписывается
- Параллельную разработку — хабы, линки и спутники загружаются независимо
- Гибкость — добавление нового источника данных означает добавление новых спутников без перепроектирования схемы

Линейность данных по всему пайплайну отслеживается в **OpenMetadata**, который
извлекает метаданные таблиц из Trino и линейность моделей из `manifest.json` dbt.

---

## 2. Компоненты инфраструктуры

### 2.1 MinIO (объектное хранилище)

S3-совместимое объектное хранилище. Все данные хранятся здесь в виде Parquet-файлов,
организованных слоем метаданных Iceberg.

**Структура бакетов:**

```
s3://warehouse/              # Корень Iceberg warehouse
  ├── staging/               # Staging views (эфемерные, без физических файлов)
  ├── raw_vault/             # Таблицы raw vault Iceberg
  │   ├── hub_patient/
  │   ├── sat_reading_value/
  │   └── ...
  ├── business_vault/        # Таблицы business vault
  └── marts/                 # Витрины данных
s3://raw/                    # Посадочная зона (сырые входящие файлы)
  ├── synthea/               # Единоразовый экспорт CSV Synthea
  │   ├── patients.csv
  │   └── conditions.csv
  └── wearable/              # Выход симулятора (ежедневно)
      └── 2026/
          └── 03/
              └── 25/
                  ├── readings.json
                  └── alerts.json
```

**Порты:** API на `9010`, Console UI на `9011` (маппинг на хост).

### 2.2 Apache Iceberg + REST Catalog

Iceberg управляет метаданными таблиц: какие Parquet-файлы принадлежат какой таблице,
версии схем, снимки для time travel и спецификации партиционирования.

**Выбор каталога: Iceberg REST Catalog** (`tabulario/iceberg-rest:latest`)

Используется REST каталог на PostgreSQL для хранения метаданных. Это предпочтительнее
Hive Metastore, потому что:

- Меньший footprint (нет Thrift-сервера, нет зависимостей от Hadoop)
- Нативный протокол Iceberg (не Hive-прокладка)
- Лучшая поддержка специфичных для Iceberg функций (мульти-табличные транзакции, views)

**PostgreSQL** хранит только метаданные каталога (расположение таблиц, схемы, снимки).

### 2.3 Trino (вычислительный движок)

Trino подключается к Iceberg REST Catalog и выполняет SQL напрямую на Parquet-файлах
в MinIO. Выполняет две роли:

1. **Движок выполнения dbt** — все dbt-модели — это операторы `CREATE TABLE AS SELECT`,
   выполняемые Trino против таблиц Iceberg
2. **Движок ad-hoc запросов** — аналитики могут запрашивать любой слой напрямую (порт `8090`)

**Конфигурация каталога Trino (`trino/catalog/iceberg.properties`):**

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.file-format=PARQUET
iceberg.compression-codec=ZSTD
iceberg.target-max-file-size=128MB

# Доступ к MinIO S3 (native-s3 API, Trino 480+)
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=${ENV:MINIO_ACCESS_KEY}
s3.aws-secret-key=${ENV:MINIO_SECRET_KEY}
s3.path-style-access=true
s3.region=us-east-1
```

### 2.4 dbt-trino (трансформации)

dbt-модели организованы в четыре слоя:

```
models/
├── staging/          # Маппинг 1:1 исходных файлов в чистые views
├── raw_vault/        # Хабы, линки, спутники (insert-only)
├── business_vault/   # Вычисленные/производные спутники
└── marts/            # Денормализованные таблицы для потребления
```

**Стратегия материализации:**

| Слой | Материализация | Обоснование |
|------|----------------|-------------|
| staging | view | Эфемерный — только очистка и приведение типов |
| raw_vault | incremental (append) | Insert-only, никаких обновлений |
| business_vault | incremental | Пересчёт только новых данных |
| marts | table (full refresh) | Широкие денормализованные таблицы для ML/BI |

**Кастомный макрос схемы** (`macros/generate_schema_name.sql`): переопределяет стандартное
поведение dbt, чтобы имена схем использовались как есть (например, `raw_vault`) без
префикса целевой схемы (который давал бы `default_raw_vault`).

**Пакеты dbt:**
- `dbt-labs/dbt_utils` — суррогатные ключи, утилиты тестирования
- `calogica/dbt_expectations` — расширенные assertions качества данных
- `calogica/dbt_date` — утилиты для работы с датами

### 2.5 Apache Airflow (оркестрация) — CeleryExecutor

Управляет ежедневным батч-циклом. Работает с **CeleryExecutor** для production-подобного
распределения задач:

| Контейнер | Роль |
|-----------|------|
| `lh-airflow-webserver` | Web UI (порт 8082) |
| `lh-airflow-scheduler` | Парсинг DAG, планирование задач |
| `lh-airflow-worker` | Выполнение задач (Celery worker) |
| `lh-redis` | Брокер сообщений Celery |
| `lh-postgres` | БД метаданных Airflow (`airflow_metadata`) + БД каталога Iceberg (`iceberg_catalog`) |

**Три DAG-а:**

**`daily_ingest`** — запускается ежедневно в 02:00 UTC:
1. Запуск симулятора носимых устройств за предыдущую дату
2. Загрузка выходных NDJSON-файлов в посадочную зону MinIO
3. Проверка наличия файлов и количества строк

**`dbt_raw_vault`** — запускается после успеха `daily_ingest`:
1. Запуск staging-моделей dbt
2. Запуск raw vault dbt (хабы → линки → спутники)
3. Запуск dbt-тестов на raw vault

**`dbt_business_vault`** — запускается после успеха `dbt_raw_vault`:
1. Запуск моделей business vault
2. Запуск mart-моделей
3. Запуск тестов качества данных

### 2.6 OpenMetadata (каталог данных и линейность)

OpenMetadata предоставляет управление метаданными и визуализацию линейности данных.
Запускается как отдельный стек (`docker-compose.openmetadata.yml`).

| Контейнер | Роль |
|-----------|------|
| `lh-om-server` | OpenMetadata API + UI (порт 8585) |
| `lh-om-mysql` | Хранилище метаданных OpenMetadata |
| `lh-om-elasticsearch` | Поиск и индексирование |

**Потоки ingestion:**
1. **Коннектор Trino** — обнаруживает все таблицы, колонки, типы данных из всех схем Iceberg
2. **Коннектор dbt** — читает `manifest.json` + `catalog.json` для построения графа линейности

**Цепочка линейности в UI:**
```
synthea.patients / wearable.readings
       ↓
   staging (stg_patients, stg_readings, ...)
       ↓
   raw_vault (hub_*, lnk_*, sat_*)
       ↓
   business_vault (bsat_*)
       ↓
   marts (mart_patient_daily_features, mart_anomaly_dashboard)
```

### 2.7 Симулятор носимых устройств

Python-пакет (`simulator/`), генерирующий реалистичные показания носимых устройств.

**Принципы проектирования:**
- **Учёт профиля пациента**: показания зависят от профиля Synthea (у диабетиков — скачки глюкозы)
- **Временной реализм**: циркадные ритмы (пульс ниже ночью), недельные паттерны
- **Управляемые аномалии**: настраиваемая вероятность аномальных событий с реалистичными сигнатурами
- **Детерминизм**: генерация на основе seed для воспроизводимых результатов
- **Ежедневный батч-вывод**: NDJSON-файлы за день, загружаемые напрямую в MinIO

**Генерируемые метрики:**

| Метрика | Единица | Нормальный диапазон |
|---------|---------|---------------------|
| heart_rate | уд/мин | 50–100 |
| spo2 | % | 95–100 |
| steps | кол-во/час | 0–2000 |
| skin_temperature | °C | 33–37 |
| sleep_stage | enum | awake/light/deep/rem |
| blood_glucose* | мг/дл | 70–140 |

*Только для пациентов с диабетом или предиабетом.

---

## 3. Поток данных: детальная последовательность

```
                  ┌─────────────┐
                  │  Synthea    │ (единоразово)
                  │  CSV-вывод  │
                  └──────┬──────┘
                         │ patients.csv, conditions.csv
                         ▼
┌─────────────────────────────────────────────────────┐
│            MinIO: s3://raw/synthea/                  │
└─────────────────────────┬───────────────────────────┘
                          │ загружается однажды при инициализации
                          ▼
┌─────────────────────────────────────────────────────┐
│  dbt staging: stg_patients, stg_conditions          │
└─────────────────────────┬───────────────────────────┘
                          │
    ┌─────────────────────┼──────────────────────┐
    ▼                     ▼                      ▼
hub_patient         hub_device            hub_metric_type
sat_patient_demo    sat_device_spec       sat_metric_def
    │                     │
    └──────────┬──────────┘
               ▼
      lnk_patient_device, lnk_device_metric
               │
         (начальное наполнение завершено)

=== ЕЖЕДНЕВНЫЙ ЦИКЛ ===

┌─────────────────┐
│ Симулятор       │ (Airflow DAG)
│ носимых         │
└────────┬────────┘
         │ readings.json, alerts.json (NDJSON)
         ▼
┌─────────────────────────────────────────────────────┐
│  MinIO: s3://raw/wearable/2026/03/25/               │
└─────────────────────────┬───────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│  dbt staging: stg_readings, stg_alerts              │
└─────────────────────────┬───────────────────────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
        hub_reading   hub_alert   sat_reading_value
                                  sat_reading_context
                                  sat_alert_detail
              │           │
              └─────┬─────┘
                    ▼
           lnk_reading_alert

                    │ dbt run (business vault)
                    ▼
        bsat_daily_vitals_agg
        bsat_anomaly_score
        bsat_patient_risk_profile

                    │ dbt run (marts)
                    ▼
        mart_patient_daily_features ──► ML пайплайн (PyArrow)
        mart_anomaly_dashboard     ──► BI
```

---

## 4. Используемые возможности Iceberg

### 4.1 Time travel для воспроизводимости ML

```sql
-- Зафиксировать обучающие данные на конкретном снимке
SELECT * FROM iceberg.marts.mart_patient_daily_features
FOR VERSION AS OF 8423947289347;

-- Или по временной метке
SELECT * FROM iceberg.marts.mart_patient_daily_features
FOR TIMESTAMP AS OF TIMESTAMP '2026-03-01 00:00:00';
```

### 4.2 Эволюция схемы

При добавлении нового типа метрики (например, артериальное давление):
1. Добавить метрику в симулятор
2. Существующая таблица `sat_reading_value` справится — та же схема, новый `metric_code`
3. Если нужны новые колонки, Iceberg добавляет их без перезаписи существующих данных

### 4.3 Истечение снимков

```sql
ALTER TABLE iceberg.raw_vault.sat_reading_value
  EXECUTE expire_snapshots(retention_threshold => '30d');
```

---

## 5. Фреймворк качества данных

### 5.1 Схемные тесты dbt

Определены в файлах `schema.yml`:
- `unique` на всех хэш-ключах хабов
- `not_null` на бизнес-ключах, `load_datetime`, `record_source`
- `relationships` между внешними ключами линков и первичными ключами хабов

### 5.2 Кастомные тесты данных (`tests/data_quality/`)

```sql
-- assert_hr_in_range.sql
SELECT * FROM {{ ref('sat_reading_value') }}
WHERE metric_code = 'heart_rate'
  AND (value < 20 OR value > 300)
```

### 5.3 Свежесть источников

```yaml
sources:
  - name: wearable
    freshness:
      warn_after: { count: 26, period: hour }
      error_after: { count: 50, period: hour }
    loaded_at_field: measured_at
```

---

## 6. Топология Docker Compose

### Основной стек (`docker-compose.yml`)

```
lh-minio          Объектное хранилище         порты: 9010 (API), 9011 (консоль)
lh-minio-init     Единоразовое создание бакетов
lh-postgres       Каталог Iceberg + метаданные Airflow  порт: 5433
lh-iceberg-rest   REST catalog сервер          порт: 8181
lh-trino          Движок запросов              порт: 8090
lh-redis          Брокер Celery
lh-airflow-init   Единоразово: миграция БД + создание admin
lh-airflow-webserver  Airflow UI               порт: 8082
lh-airflow-scheduler  Планировщик DAG
lh-airflow-worker     Celery task worker
```

### Стек OpenMetadata (`docker-compose.openmetadata.yml`)

```
lh-om-mysql          Хранилище метаданных OpenMetadata   порт: 3306
lh-om-elasticsearch  Поисковой движок                    порт: 9200
lh-om-migrate        Единоразово: миграция схемы БД
lh-om-server         OpenMetadata API + UI               порты: 8585, 8586
```

Оба стека разделяют сеть Docker `health-iot-lakehouse_lakehouse-net`,
поэтому OpenMetadata может обращаться к Trino по адресу `trino:8080`.

Суммарное потребление памяти: ~5–6 ГБ. Требует Docker Desktop, настроенный на ≥ 8 ГБ.

---

## 7. Безопасность и управление данными

Поскольку это портфолийный проект с синтетическими данными, безопасность упрощена:
- Доступ к MinIO через статические учётные данные в `.env`
- Trino без аутентификации
- OpenMetadata с базовой аутентификацией (`admin@openmetadata.org` / `admin`)
- Без шифрования данных в покое

В production-среде необходимо добавить:
- IAM-доступ к S3 (или политики MinIO)
- Аутентификацию Trino + LDAP/OAuth
- Управление доступом на уровне колонок для PII-полей
- Шифрование Parquet для PHI-колонок
- Аудит-логирование всего доступа к данным

---

## 8. Соображения о производительности

| Проблема | Подход |
|----------|--------|
| Размер Parquet-файлов | Целевой размер 128 МБ (`iceberg.target-max-file-size=128MB`) |
| Проблема мелких файлов | Iceberg `rewrite_data_files` после ежедневных загрузок |
| Производительность запросов | Partition pruning на `day(measured_at)` |
| dbt incremental | Фильтр `load_datetime > max(load_datetime)` для спутников |
| Trino JVM | `-Xmx1536M` в `jvm.config` для локальной разработки |

---

## 9. Точки расширения

- **Слой реального времени**: Добавить Kafka + Flink для стриминга, те же Iceberg-таблицы
- **Feature store**: Интеграция Feast для онлайн-сервинга признаков
- **Model registry**: MLflow для отслеживания экспериментов
- **BI-дашборды**: Apache Superset, подключённый к Trino
- **Data contracts**: Great Expectations или Soda Core на границе посадочной зоны
