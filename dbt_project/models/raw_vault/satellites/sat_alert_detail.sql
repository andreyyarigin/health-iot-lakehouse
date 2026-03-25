{{
  config(
    materialized = 'incremental',
    unique_key = ['alert_hk', 'load_datetime'],
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  sat_alert_detail — descriptive attributes of alert events.

  Stores the full clinical context of a threshold breach: which metric,
  what threshold, what actual value, severity, and acknowledgment status.

  acknowledged and acknowledged_at default to FALSE/NULL — the simulator
  does not generate acknowledgment events. These columns exist for
  future workflow integration.
*/

select
    {{ dbt_utils.generate_surrogate_key(['alert_id']) }}        as alert_hk,
    load_datetime,
    {{ dbt_utils.generate_surrogate_key([
        'alert_type',
        'severity',
        'metric_code',
        'threshold_value',
        'actual_value'
    ]) }}                                                         as hash_diff,
    triggered_at                                                  as effective_from,
    record_source,
    alert_type,
    severity,
    metric_code,
    cast(threshold_value as decimal(10, 4))                       as threshold_value,
    cast(actual_value    as decimal(10, 4))                       as actual_value,
    triggered_at,
    cast(false           as boolean)                              as acknowledged,
    cast(null as timestamp(6))                            as acknowledged_at

from {{ ref('stg_alerts') }}

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
