{{
  config(
    materialized = 'view',
    schema = 'staging'
  )
}}

/*
  stg_patients — clean and type-cast Synthea patients.csv.

  Reads from the Hive external table that points at s3://raw/synthea/patients.csv.
  Enforces canonical column names, casts to correct types, and adds load metadata.
*/

select
    cast(id            as varchar)        as patient_id,
    cast(birthdate     as date)           as birth_date,
    cast(gender        as varchar)        as gender,
    cast(race          as varchar)        as race,
    cast(ethnicity     as varchar)        as ethnicity,
    cast(city          as varchar)        as city,
    cast(state         as varchar)        as state,
    cast(zip           as varchar)        as zip,
    try_cast(bmi       as decimal(5, 2))  as bmi,
    cast(first         as varchar)        as first_name,
    cast(last          as varchar)        as last_name,
    cast(current_timestamp as timestamp(6))                     as load_datetime,
    'synthea'                             as record_source

from {{ source('raw_synthea', 'patients') }}

where id is not null
