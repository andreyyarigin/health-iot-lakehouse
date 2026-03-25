{{
  config(
    materialized = 'incremental',
    unique_key = 'device_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  hub_device — hub for physical wearable devices.

  Business key: device_serial (e.g. "GRM-W0042", "CGM-0042").
  Devices are first seen when a reading arrives from them; we derive distinct
  device serials from the readings staging layer.
*/

select
    {{ dbt_utils.generate_surrogate_key(['device_serial']) }}  as device_hk,
    device_serial                                               as device_serial_bk,
    load_datetime,
    'wearable_simulator'                                        as record_source

from (
    select distinct
        device_serial,
        min(load_datetime) as load_datetime
    from {{ ref('stg_readings') }}
    where device_serial is not null
    group by device_serial
) distinct_devices

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
