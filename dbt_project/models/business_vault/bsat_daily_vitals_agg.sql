{{
  config(
    materialized = 'incremental',
    unique_key = ['patient_hk', 'metric_code', 'report_date'],
    incremental_strategy = 'append',
    schema = 'business_vault'
  )
}}

/*
  bsat_daily_vitals_agg — daily aggregate statistics per patient per metric.

  Computes descriptive statistics (mean, std, min, max, percentiles) over all
  readings for a patient-metric on a given calendar date. This is the primary
  input to anomaly scoring and the ML feature table.

  completeness_pct is computed as actual readings / expected readings based on
  the known sampling frequency per metric type:
    heart_rate / spo2:  288 readings/day  (every 5 min)
    steps:               24 readings/day  (hourly)
    skin_temperature:    96 readings/day  (every 15 min)
    blood_glucose:       96 readings/day  (every 15 min, diabetics only)
    sleep_stage:        varies (~480 during sleep hours)
*/

with daily_counts as (
    select
        patient_hk,
        metric_code,
        cast(measured_at as date)                                            as report_date,
        count(*)                                                             as reading_count,
        avg(value)                                                           as value_mean,
        stddev(value)                                                        as value_std,
        min(value)                                                           as value_min,
        max(value)                                                           as value_max,
        approx_percentile(value, 0.50)                                       as value_median,
        approx_percentile(value, 0.05)                                       as value_p05,
        approx_percentile(value, 0.95)                                       as value_p95,
        current_timestamp                                                    as load_datetime

    from {{ ref('sat_reading_value') }}

    {% if is_incremental() %}
    where load_datetime > (select max(load_datetime) from {{ this }})
    {% endif %}

    group by
        patient_hk,
        metric_code,
        cast(measured_at as date)
)

select
    patient_hk,
    metric_code,
    report_date,
    load_datetime,
    'business_vault'                                                          as record_source,
    reading_count,
    cast(value_mean   as decimal(10, 4))                                     as value_mean,
    cast(value_std    as decimal(10, 4))                                     as value_std,
    cast(value_min    as decimal(10, 4))                                     as value_min,
    cast(value_max    as decimal(10, 4))                                     as value_max,
    cast(value_median as decimal(10, 4))                                     as value_median,
    cast(value_p05    as decimal(10, 4))                                     as value_p05,
    cast(value_p95    as decimal(10, 4))                                     as value_p95,

    -- Completeness: readings received vs expected readings per day
    cast(
        reading_count * 100.0 / case metric_code
            when 'heart_rate'      then 288.0
            when 'spo2'            then 288.0
            when 'steps'           then  24.0
            when 'skin_temperature' then  96.0
            when 'blood_glucose'   then  96.0
            when 'sleep_stage'     then 480.0
            else reading_count
        end
        as decimal(5, 2)
    )                                                                         as completeness_pct

from daily_counts
