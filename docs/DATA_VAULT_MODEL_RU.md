# Модель Data Vault 2.0

> Полный справочник по сущностям Data Vault для health-iot-lakehouse.

---

## Соглашения об именовании

| Тип сущности | Префикс | Пример |
|-------------|---------|--------|
| Хаб | `hub_` | `hub_patient` |
| Линк | `lnk_` | `lnk_patient_device` |
| Спутник (raw) | `sat_` | `sat_patient_demographics` |
| Спутник (business) | `bsat_` | `bsat_daily_vitals_agg` |
| Витрина данных | `mart_` | `mart_patient_daily_features` |

**Стандартные колонки** присутствуют в каждой таблице Data Vault:

| Колонка | Тип | Описание |
|---------|-----|----------|
| `*_hk` | VARCHAR(32) | MD5 хэш-ключ (первичный ключ для хабов/линков) |
| `load_datetime` | TIMESTAMP | Когда запись была загружена |
| `record_source` | VARCHAR | Идентификатор исходной системы |

Спутники дополнительно содержат:

| Колонка | Тип | Описание |
|---------|-----|----------|
| `hash_diff` | VARCHAR(32) | MD5 всех описательных атрибутов (обнаружение изменений) |
| `effective_from` | TIMESTAMP | Когда эта версия стала активной |

---

## Хабы

### hub_patient

Центральная бизнес-сущность — отслеживаемый пациент.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | MD5(patient_bk) |
| `patient_bk` | VARCHAR | UUID пациента Synthea (бизнес-ключ) |
| `load_datetime` | TIMESTAMP | Временная метка первого появления |
| `record_source` | VARCHAR | `synthea` |

### hub_device

Физическое носимое устройство (смарт-часы, глюкометр, пульсоксиметр).

| Колонка | Тип | Описание |
|---------|-----|----------|
| `device_hk` | VARCHAR(32) | MD5(device_serial_bk) |
| `device_serial_bk` | VARCHAR | Серийный номер устройства |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `wearable_simulator` |

### hub_metric_type

Справочная сущность для типов медицинских измерений.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `metric_type_hk` | VARCHAR(32) | MD5(metric_code_bk) |
| `metric_code_bk` | VARCHAR | Например, `heart_rate`, `spo2`, `steps`, `skin_temp`, `blood_glucose`, `sleep_stage` |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `system_seed` |

### hub_reading

Отдельное событие измерения — одна точка данных с одного устройства в один момент времени.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `reading_hk` | VARCHAR(32) | MD5(reading_id_bk) |
| `reading_id_bk` | VARCHAR | Уникальный идентификатор измерения (генерируется симулятором) |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `wearable_simulator` |

### hub_alert

Событие тревоги, сработавшей при превышении показателем порогового значения.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `alert_hk` | VARCHAR(32) | MD5(alert_id_bk) |
| `alert_id_bk` | VARCHAR | Уникальный идентификатор тревоги |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `wearable_simulator` |

---

## Линки

### lnk_patient_device

Какой пациент носит какое устройство. У пациента может быть несколько устройств;
устройство принадлежит ровно одному пациенту.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_device_hk` | VARCHAR(32) | MD5(patient_bk \|\| device_serial_bk) |
| `patient_hk` | VARCHAR(32) | FK → hub_patient |
| `device_hk` | VARCHAR(32) | FK → hub_device |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | |

### lnk_device_metric

Какое устройство регистрирует какие типы метрик. Смарт-часы могут регистрировать HR, SpO2,
шаги и температуру кожи; CGM регистрирует только уровень глюкозы.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `device_metric_hk` | VARCHAR(32) | MD5(device_serial_bk \|\| metric_code_bk) |
| `device_hk` | VARCHAR(32) | FK → hub_device |
| `metric_type_hk` | VARCHAR(32) | FK → hub_metric_type |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | |

### lnk_reading_alert

Связывает конкретное измерение с тревогой, которую оно вызвало. Не каждое измерение
генерирует тревогу — только те, которые превышают пороги.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `reading_alert_hk` | VARCHAR(32) | MD5(reading_id_bk \|\| alert_id_bk) |
| `reading_hk` | VARCHAR(32) | FK → hub_reading |
| `alert_hk` | VARCHAR(32) | FK → hub_alert |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | |

---

## Спутники (Raw Vault)

### sat_patient_demographics

Описательные атрибуты пациента. Источник: Synthea. Меняются редко
(например, обновление адреса, новый диагноз).

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | FK → hub_patient |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | `synthea` |
| `birth_date` | DATE | |
| `gender` | VARCHAR | M / F |
| `race` | VARCHAR | |
| `ethnicity` | VARCHAR | |
| `city` | VARCHAR | |
| `state` | VARCHAR | |
| `zip` | VARCHAR | |
| `bmi` | DECIMAL(5,2) | |
| `chronic_conditions` | VARCHAR | Коды SNOMED через запятую |

### sat_device_spec

Технические характеристики носимого устройства.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `device_hk` | VARCHAR(32) | FK → hub_device |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | |
| `device_model` | VARCHAR | Например, `Garmin Venu 3`, `Dexcom G7` |
| `manufacturer` | VARCHAR | |
| `firmware_version` | VARCHAR | |
| `battery_level_pct` | INTEGER | Последний известный уровень заряда % |

### sat_metric_definition

Справочные данные для каждого типа метрики — что измеряется, единицы, нормальные диапазоны.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `metric_type_hk` | VARCHAR(32) | FK → hub_metric_type |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | |
| `metric_name` | VARCHAR | Человекочитаемое название |
| `unit` | VARCHAR | уд/мин, %, кол-во, °C, мг/дл |
| `normal_range_low` | DECIMAL | |
| `normal_range_high` | DECIMAL | |
| `sampling_frequency` | VARCHAR | `5min`, `15min`, `hourly`, `per_epoch` |

### sat_reading_value

Основная таблица фактов — фактические значения измерений. Это наиболее нагруженный
спутник, получающий тысячи строк на пациента в день.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `reading_hk` | VARCHAR(32) | FK → hub_reading |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | |
| `patient_hk` | VARCHAR(32) | Денормализован для производительности запросов |
| `device_hk` | VARCHAR(32) | Денормализован для производительности запросов |
| `metric_code` | VARCHAR | `heart_rate`, `spo2` и т.д. |
| `value` | DECIMAL(10,4) | Значение измерения |
| `unit` | VARCHAR | |
| `quality_flag` | VARCHAR | `good`, `noisy`, `missing`, `interpolated` |
| `measured_at` | TIMESTAMP | Когда было сделано измерение |

**Партиционирование:** `day(measured_at)` — критично для производительности запросов,
поскольку большинство запросов фильтруют по диапазону дат.

**Примечание о денормализации:** `patient_hk` и `device_hk` денормализованы в
этот спутник (вместо требования соединения через линки), потому что эта таблица
используется миллиарды раз в агрегациях. Это принятый паттерн Data Vault 2.0
для высоконагруженных спутников.

### sat_reading_context

Контекстная информация об измерении — чем занимался пациент, где находился.
Отделён от `sat_reading_value`, потому что контекст может быть недоступен
для каждого измерения и меняется с другой гранулярностью.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `reading_hk` | VARCHAR(32) | FK → hub_reading |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | |
| `activity_type` | VARCHAR | `resting`, `walking`, `running`, `sleeping`, `driving` |
| `location_type` | VARCHAR | `home`, `outdoor`, `gym`, `office` |
| `mood_score` | INTEGER | 1–10 самооценка (nullable) |

### sat_alert_detail

Детали событий тревог — какой порог был превышен, серьёзность, статус подтверждения.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `alert_hk` | VARCHAR(32) | FK → hub_alert |
| `load_datetime` | TIMESTAMP | |
| `hash_diff` | VARCHAR(32) | |
| `effective_from` | TIMESTAMP | |
| `record_source` | VARCHAR | |
| `alert_type` | VARCHAR | `threshold_breach`, `trend_anomaly`, `missing_data` |
| `severity` | VARCHAR | `info`, `warning`, `critical` |
| `metric_code` | VARCHAR | Какая метрика вызвала тревогу |
| `threshold_value` | DECIMAL | Превышённый порог |
| `actual_value` | DECIMAL | Значение, превысившее порог |
| `triggered_at` | TIMESTAMP | |
| `acknowledged` | BOOLEAN | Просмотрено ли клиницистом |
| `acknowledged_at` | TIMESTAMP | |

---

## Спутники (Business Vault)

### bsat_daily_vitals_agg

Дневная агрегированная статистика на пациента на метрику. Вычисляется dbt из
`sat_reading_value`.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | |
| `metric_code` | VARCHAR | |
| `report_date` | DATE | |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `business_vault` |
| `reading_count` | INTEGER | Количество измерений за день |
| `value_mean` | DECIMAL | |
| `value_std` | DECIMAL | |
| `value_min` | DECIMAL | |
| `value_max` | DECIMAL | |
| `value_median` | DECIMAL | |
| `value_p05` | DECIMAL | 5-й перцентиль |
| `value_p95` | DECIMAL | 95-й перцентиль |
| `completeness_pct` | DECIMAL | % полученных ожидаемых измерений |

### bsat_anomaly_score

Ежедневная оценка аномалий на пациента. Использует логику на основе правил
(пороги, z-оценки относительно собственного базового уровня пациента).

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | |
| `report_date` | DATE | |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `business_vault` |
| `hr_zscore` | DECIMAL | Z-оценка дневного среднего HR vs 30-дневный базовый уровень |
| `spo2_zscore` | DECIMAL | |
| `steps_zscore` | DECIMAL | |
| `composite_anomaly_score` | DECIMAL | Взвешенная сумма z-оценок |
| `anomaly_flag` | BOOLEAN | True если composite score превышает порог |
| `contributing_metrics` | VARCHAR | Какие метрики внесли наибольший вклад |

### bsat_patient_risk_profile

Профиль риска пациента, объединяющий демографию, хронические заболевания
и недавние тенденции жизненных показателей.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | |
| `report_date` | DATE | |
| `load_datetime` | TIMESTAMP | |
| `record_source` | VARCHAR | `business_vault` |
| `age` | INTEGER | Вычислено из birth_date |
| `condition_count` | INTEGER | Количество активных хронических заболеваний |
| `has_diabetes` | BOOLEAN | |
| `has_hypertension` | BOOLEAN | |
| `has_cardiac_condition` | BOOLEAN | |
| `avg_anomaly_score_7d` | DECIMAL | 7-дневное скользящее среднее |
| `anomaly_events_30d` | INTEGER | Количество аномальных дней за последние 30 |
| `data_completeness_7d` | DECIMAL | Средняя полнота за последние 7 дней |
| `risk_tier` | VARCHAR | `low`, `moderate`, `high`, `critical` |

---

## Витрины данных

### mart_patient_daily_features

**ML feature table.** Одна строка на пациента на день, со всеми признаками,
необходимыми для предсказания аномалий. Именно эту таблицу читает PyArrow для обучения модели.

| Колонка | Тип | Источник |
|---------|-----|----------|
| `patient_hk` | VARCHAR(32) | hub_patient |
| `report_date` | DATE | |
| `age` | INTEGER | bsat_patient_risk_profile |
| `gender` | VARCHAR | sat_patient_demographics |
| `bmi` | DECIMAL | sat_patient_demographics |
| `has_diabetes` | BOOLEAN | bsat_patient_risk_profile |
| `has_hypertension` | BOOLEAN | bsat_patient_risk_profile |
| `has_cardiac_condition` | BOOLEAN | bsat_patient_risk_profile |
| `condition_count` | INTEGER | bsat_patient_risk_profile |
| `hr_mean` | DECIMAL | bsat_daily_vitals_agg |
| `hr_std` | DECIMAL | bsat_daily_vitals_agg |
| `hr_min` | DECIMAL | bsat_daily_vitals_agg |
| `hr_max` | DECIMAL | bsat_daily_vitals_agg |
| `hr_p05` | DECIMAL | bsat_daily_vitals_agg |
| `hr_p95` | DECIMAL | bsat_daily_vitals_agg |
| `spo2_mean` | DECIMAL | bsat_daily_vitals_agg |
| `spo2_min` | DECIMAL | bsat_daily_vitals_agg |
| `steps_total` | INTEGER | bsat_daily_vitals_agg |
| `skin_temp_mean` | DECIMAL | bsat_daily_vitals_agg |
| `glucose_mean` | DECIMAL | bsat_daily_vitals_agg (nullable) |
| `glucose_std` | DECIMAL | bsat_daily_vitals_agg (nullable) |
| `sleep_duration_hrs` | DECIMAL | bsat_daily_vitals_agg |
| `hr_mean_7d_avg` | DECIMAL | 7-дневное скользящее окно |
| `hr_mean_delta_1d` | DECIMAL | Дневное изменение |
| `spo2_min_7d_avg` | DECIMAL | 7-дневное скользящее окно |
| `steps_total_7d_avg` | DECIMAL | 7-дневное скользящее окно |
| `composite_anomaly_score` | DECIMAL | bsat_anomaly_score |
| `anomaly_events_30d` | INTEGER | bsat_patient_risk_profile |
| `data_completeness` | DECIMAL | bsat_daily_vitals_agg |
| `risk_tier` | VARCHAR | bsat_patient_risk_profile |
| `anomaly_next_24h` | BOOLEAN | **ЦЕЛЕВАЯ ПЕРЕМЕННАЯ** — метка для ML |

**Логика целевой переменной:** `anomaly_next_24h` равна TRUE, если у пациента
`anomaly_flag=TRUE` на `report_date + 1`. Это создаёт задачу обучения с учителем —
используем признаки сегодняшнего дня для предсказания аномалии завтра.

### mart_anomaly_dashboard

Агрегированное представление для BI-дашбордов. Одна строка на пациента с текущим статусом.

| Колонка | Тип | Описание |
|---------|-----|----------|
| `patient_bk` | VARCHAR | Человекочитаемый ID пациента |
| `patient_name` | VARCHAR | Синтетическое имя из Synthea |
| `age` | INTEGER | |
| `risk_tier` | VARCHAR | Текущий уровень риска |
| `latest_hr` | DECIMAL | Последнее значение пульса |
| `latest_spo2` | DECIMAL | Последнее значение SpO2 |
| `anomaly_score_today` | DECIMAL | Сегодняшний composite score |
| `anomaly_trend` | VARCHAR | `improving`, `stable`, `worsening` |
| `days_since_last_anomaly` | INTEGER | |
| `active_alerts` | INTEGER | Неподтверждённые тревоги |

---

## Сводка связей сущностей

```
hub_patient ──┬── sat_patient_demographics
              │
              ├── lnk_patient_device ── hub_device ── sat_device_spec
              │
              │   hub_device ── lnk_device_metric ── hub_metric_type ── sat_metric_def
              │
              └── (через линки) ── hub_reading ──┬── sat_reading_value
                                                 ├── sat_reading_context
                                                 │
                                                 └── lnk_reading_alert ── hub_alert ── sat_alert_detail
```

---

## Генерация хэш-ключей

Все хэш-ключи используют MD5 для простоты (допустимо для Data Vault — не криптографический
хэш, а просто суррогат). В dbt:

```sql
-- Хэш-ключ хаба
{{ dbt_utils.generate_surrogate_key(['patient_bk']) }} AS patient_hk

-- Хэш-ключ линка (конкатенация бизнес-ключей из обоих хабов)
{{ dbt_utils.generate_surrogate_key(['patient_bk', 'device_serial_bk']) }} AS patient_device_hk

-- Hash diff спутника (обнаружение изменений)
{{ dbt_utils.generate_surrogate_key(['gender', 'city', 'state', 'bmi', 'chronic_conditions']) }} AS hash_diff
```

---

## Паттерн инкрементальной загрузки

Спутники используют инкрементальную материализацию dbt с логикой только добавления:

```sql
-- Пример: sat_reading_value
{{
  config(
    materialized='incremental',
    unique_key=['reading_hk', 'load_datetime'],
    incremental_strategy='append'
  )
}}

SELECT
  reading_hk,
  load_datetime,
  hash_diff,
  effective_from,
  record_source,
  patient_hk,
  device_hk,
  metric_code,
  value,
  unit,
  quality_flag,
  measured_at
FROM {{ ref('stg_readings') }}

{% if is_incremental() %}
WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
{% endif %}
```

Для спутников с обнаружением изменений (например, `sat_patient_demographics`) добавляется
проверка `hash_diff`, чтобы вставлять только при реальном изменении описательной нагрузки:

```sql
{% if is_incremental() %}
WHERE load_datetime > (SELECT MAX(load_datetime) FROM {{ this }})
  AND hash_diff NOT IN (
    SELECT hash_diff FROM {{ this }}
    WHERE patient_hk = stg.patient_hk
      AND effective_from = (
        SELECT MAX(effective_from) FROM {{ this }} sub
        WHERE sub.patient_hk = stg.patient_hk
      )
  )
{% endif %}
```
