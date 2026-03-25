{{
  config(
    materialized = 'incremental',
    unique_key = ['device_hk', 'load_datetime'],
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  sat_device_spec — technical specifications of wearable devices.

  The simulator encodes device model and manufacturer in the serial number
  prefix:
    GRM-Wxxxx  → Garmin smartwatch (round-robin across Garmin Venu 3,
                  Apple Watch Ultra 2, Withings ScanWatch 2)
    CGM-xxxx   → Dexcom G7 (CGM device assigned to diabetic patients)

  Because the raw landing zone does not include a separate device_status.json
  parse in staging, we derive device_model and manufacturer from the serial
  number prefix pattern using a CASE expression.

  battery_level_pct is nullable — not available from readings alone.
  firmware_version is set to a static placeholder.

  Change detection via hash_diff. A new row is only inserted when the
  descriptive payload changes.
*/

with devices as (
    select distinct
        device_serial,
        min(load_datetime) as load_datetime,
        'wearable_simulator' as record_source
    from {{ ref('stg_readings') }}
    where device_serial is not null
    group by device_serial
),

enriched as (
    select
        device_serial,
        load_datetime,
        record_source,

        -- Derive device model from serial prefix
        case
            when device_serial like 'CGM-%'   then 'Dexcom G7'
            when device_serial like 'GRM-W%'
                -- Round-robin across smartwatch models based on numeric suffix
                and try_cast(regexp_extract(device_serial, '\d+$', 0) as integer) % 3 = 0
                                               then 'Garmin Venu 3'
            when device_serial like 'GRM-W%'
                and try_cast(regexp_extract(device_serial, '\d+$', 0) as integer) % 3 = 1
                                               then 'Apple Watch Ultra 2'
            when device_serial like 'GRM-W%'  then 'Withings ScanWatch 2'
            else 'Unknown'
        end as device_model,

        -- Derive manufacturer from device model
        case
            when device_serial like 'CGM-%'   then 'Dexcom'
            when device_serial like 'GRM-W%'
                and try_cast(regexp_extract(device_serial, '\d+$', 0) as integer) % 3 = 0
                                               then 'Garmin'
            when device_serial like 'GRM-W%'
                and try_cast(regexp_extract(device_serial, '\d+$', 0) as integer) % 3 = 1
                                               then 'Apple'
            when device_serial like 'GRM-W%'  then 'Withings'
            else 'Unknown'
        end as manufacturer,

        '1.0.0'      as firmware_version,
        cast(null as integer) as battery_level_pct

    from devices
),

with_keys as (
    select
        {{ dbt_utils.generate_surrogate_key(['device_serial']) }}  as device_hk,
        load_datetime,
        {{ dbt_utils.generate_surrogate_key([
            'device_model',
            'manufacturer',
            'firmware_version'
        ]) }}                                                        as hash_diff,
        load_datetime                                                as effective_from,
        record_source,
        device_model,
        manufacturer,
        firmware_version,
        battery_level_pct
    from enriched
)

select stg.*
from with_keys stg

{% if is_incremental() %}
left join (
    select device_hk, max(effective_from) as max_effective_from
    from {{ this }}
    group by device_hk
) latest on stg.device_hk = latest.device_hk
left join {{ this }} last_rec
    on last_rec.device_hk = latest.device_hk
    and last_rec.effective_from = latest.max_effective_from
where stg.load_datetime > (select max(load_datetime) from {{ this }})
  and (last_rec.hash_diff is null or last_rec.hash_diff != stg.hash_diff)
{% endif %}
