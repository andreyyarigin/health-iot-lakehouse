{{
  config(
    materialized = 'incremental',
    unique_key = 'patient_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  hub_patient — central business entity hub for patients.

  Business key: patient_id (Synthea UUID).
  One row per distinct patient ever seen. Insert-only (append strategy).
*/

select
    {{ dbt_utils.generate_surrogate_key(['patient_id']) }}  as patient_hk,
    patient_id                                              as patient_bk,
    load_datetime,
    record_source

from {{ ref('stg_patients') }}

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
