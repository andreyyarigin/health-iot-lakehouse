{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_alerts — parse and clean daily alert NDJSON.

  The simulator's AlertGenerator emits one alert dict per breach window:
    {
      "alert_id": "...",
      "patient_id": "...",
      "device_serial": "...",
      "alert_type": "threshold_breach",
      "severity": "critical",
      "metric_code": "heart_rate",
      "threshold_value": 150.0,
      "actual_value": 162.4,
      "triggered_at": "2026-03-25T10:00:00Z",
      "reading_id": "..."
    }
*/

select
    cast(alert_id        as varchar)         as alert_id,
    cast(patient_id      as varchar)         as patient_id,
    cast(device_serial   as varchar)         as device_serial,
    cast(alert_type      as varchar)         as alert_type,
    cast(severity        as varchar)         as severity,
    cast(metric_code     as varchar)         as metric_code,
    cast(threshold_value as decimal(10, 4))  as threshold_value,
    cast(actual_value    as decimal(10, 4))  as actual_value,
    cast(from_iso8601_timestamp(triggered_at) as timestamp(6))       as triggered_at,
    cast(reading_id      as varchar)         as reading_id,
    cast(current_timestamp as timestamp(6))                        as load_datetime,
    'wearable_simulator'                     as record_source

from {{ source('raw_wearable', 'alerts') }}

where alert_id  is not null
  and patient_id is not null
