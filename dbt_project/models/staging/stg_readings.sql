{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_readings — parse and clean daily wearable readings NDJSON.

  The simulator writes one JSON object per line with a nested "context" object:
    {
      "reading_id": "...",
      "patient_id": "...",
      "device_serial": "...",
      "metric_code": "heart_rate",
      "value": 78.4,
      "unit": "bpm",
      "quality_flag": "good",
      "measured_at": "2026-03-25T14:30:00Z",
      "context": {
        "activity": "walking",
        "location_type": "outdoor"
      }
    }

  Trino reads the NDJSON as a Hive table with a JSON SerDe. The nested "context"
  fields are accessed via JSON path extraction.

  Records with quality_flag = 'missing' are excluded at this stage.
*/

select
    cast(reading_id    as varchar)          as reading_id,
    cast(patient_id    as varchar)          as patient_id,
    cast(device_serial as varchar)          as device_serial,
    cast(metric_code   as varchar)          as metric_code,
    cast(value         as decimal(10, 4))   as value,
    cast(unit          as varchar)          as unit,
    cast(quality_flag  as varchar)          as quality_flag,
    cast(from_iso8601_timestamp(measured_at) as timestamp(6))        as measured_at,
    cast(
        json_extract_scalar(context, '$.activity')
        as varchar
    )                                       as activity_type,
    cast(
        json_extract_scalar(context, '$.location_type')
        as varchar
    )                                       as location_type,
    cast(current_timestamp as timestamp(6))                       as load_datetime,
    'wearable_simulator'                    as record_source

from {{ source('raw_wearable', 'readings') }}

where quality_flag != 'missing'
  and reading_id is not null
  and patient_id is not null
