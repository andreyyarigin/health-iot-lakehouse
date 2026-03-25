{{
  config(
    materialized = 'incremental',
    unique_key = ['reading_hk', 'load_datetime'],
    incremental_strategy = 'append',
    schema = 'raw_vault',
    properties = {
      "partitioning": "ARRAY['day(measured_at)']",
      "format": "'PARQUET'"
    }
  )
}}

/*
  sat_reading_value — the core high-volume satellite for actual measurement values.

  One row per reading. Denormalized patient_hk and device_hk are included for
  query performance — avoids expensive joins through links for aggregation queries.
  This is an accepted Data Vault 2.0 pattern for high-volume satellites.

  Partitioned by day(measured_at) for efficient date-range queries.
  Append-only — readings are immutable once written.

  hash_diff is over the full measurement payload (value, unit, quality_flag).
*/

select
    {{ dbt_utils.generate_surrogate_key(['reading_id']) }}                   as reading_hk,
    load_datetime,
    {{ dbt_utils.generate_surrogate_key([
        'metric_code',
        'value',
        'unit',
        'quality_flag'
    ]) }}                                                                      as hash_diff,
    measured_at                                                                as effective_from,
    record_source,

    -- Denormalized hub keys for query performance
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }}                   as patient_hk,
    {{ dbt_utils.generate_surrogate_key(['device_serial']) }}                as device_hk,

    metric_code,
    cast(value       as decimal(10, 4))                                       as value,
    unit,
    quality_flag,
    measured_at

from {{ ref('stg_readings') }}

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
