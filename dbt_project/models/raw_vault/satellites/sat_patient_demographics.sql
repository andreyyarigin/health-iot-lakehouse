{{
  config(
    materialized = 'incremental',
    unique_key = ['patient_hk', 'load_datetime'],
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  sat_patient_demographics — descriptive attributes of a patient.

  Source: stg_patients (Synthea CSV) joined with stg_conditions to build
  a comma-separated list of chronic condition SNOMED codes.

  Change detection: hash_diff computed over all descriptive columns.
  A new row is only inserted when the payload actually changes (e.g. new
  condition diagnosis or address update). Effective_from marks when this
  version became active.
*/

with patients as (
    select * from {{ ref('stg_patients') }}
),

conditions_agg as (
    select
        patient_id,
        array_join(
            array_agg(condition_code order by condition_code),
            ','
        ) as chronic_conditions
    from {{ ref('stg_conditions') }}
    where resolved_date is null  -- only active/unresolved conditions
    group by patient_id
),

combined as (
    select
        p.patient_id,
        p.birth_date,
        p.gender,
        p.race,
        p.ethnicity,
        p.city,
        p.state,
        p.zip,
        p.bmi,
        coalesce(c.chronic_conditions, '') as chronic_conditions,
        p.load_datetime,
        p.record_source
    from patients p
    left join conditions_agg c
        on p.patient_id = c.patient_id
),

with_keys as (
    select
        {{ dbt_utils.generate_surrogate_key(['patient_id']) }}  as patient_hk,
        load_datetime,
        {{ dbt_utils.generate_surrogate_key([
            'gender',
            'city',
            'state',
            'zip',
            'bmi',
            'chronic_conditions'
        ]) }}                                                    as hash_diff,
        load_datetime                                            as effective_from,
        record_source,
        birth_date,
        gender,
        race,
        ethnicity,
        city,
        state,
        zip,
        bmi,
        chronic_conditions
    from combined
)

select stg.*
from with_keys stg

{% if is_incremental() %}
left join (
    select patient_hk, max(effective_from) as max_effective_from
    from {{ this }}
    group by patient_hk
) latest on stg.patient_hk = latest.patient_hk
left join {{ this }} last_rec
    on last_rec.patient_hk = latest.patient_hk
    and last_rec.effective_from = latest.max_effective_from
where stg.load_datetime > (select max(load_datetime) from {{ this }})
  and (last_rec.hash_diff is null or last_rec.hash_diff != stg.hash_diff)
{% endif %}
