{{
  config(
    materialized = 'incremental',
    unique_key = 'device_metric_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  lnk_device_metric — links devices to the metric types they capture.

  Derived from stg_readings: a reading implies that the originating device
  is capable of capturing that metric type. Distinct device+metric pairs
  are collapsed to one link record per combination.
*/

select distinct
    {{ dbt_utils.generate_surrogate_key(['device_serial', 'metric_code']) }}  as device_metric_hk,
    {{ dbt_utils.generate_surrogate_key(['device_serial']) }}                  as device_hk,
    {{ dbt_utils.generate_surrogate_key(['metric_code']) }}                    as metric_type_hk,
    min(load_datetime) over (
        partition by device_serial, metric_code
    )                                                                            as load_datetime,
    record_source

from {{ ref('stg_readings') }}

where device_serial is not null
  and metric_code   is not null

{% if is_incremental() %}
  and load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
