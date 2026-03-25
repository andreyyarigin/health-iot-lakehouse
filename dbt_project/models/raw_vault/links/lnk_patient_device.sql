{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_device_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  lnk_patient_device — links patients to their wearable devices.

  Derived from stg_readings: any reading that has both a patient_id and a
  device_serial implies that patient wears that device on that load date.
  We take distinct combinations to avoid duplicating the link on every reading.
*/

select distinct
    {{ dbt_utils.generate_surrogate_key(['patient_id', 'device_serial']) }}  as patient_device_hk,
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }}                   as patient_hk,
    {{ dbt_utils.generate_surrogate_key(['device_serial']) }}                as device_hk,
    min(load_datetime) over (
        partition by patient_id, device_serial
    )                                                                         as load_datetime,
    record_source

from {{ ref('stg_readings') }}

where patient_id    is not null
  and device_serial is not null

{% if is_incremental() %}
  and load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
