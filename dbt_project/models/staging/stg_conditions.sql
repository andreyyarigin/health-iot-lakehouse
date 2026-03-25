{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_conditions — clean and type-cast Synthea conditions.csv.

  Synthea uses the column name "patient" (not "patient_id") and "code"
  (not "condition_code") for the primary identifier columns. This model
  renames them to the project-standard names.
*/

select
    cast(patient      as varchar)   as patient_id,
    cast(code         as varchar)   as condition_code,
    cast(description  as varchar)   as condition_description,
    cast(start        as date)      as onset_date,
    try_cast(stop     as date)      as resolved_date,
    cast(current_timestamp as timestamp(6))               as load_datetime,
    'synthea'                       as record_source

from {{ source('raw_synthea', 'conditions') }}

where patient is not null
  and code is not null
