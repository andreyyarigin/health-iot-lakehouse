{{
  config(
    materialized = 'incremental',
    unique_key = 'metric_type_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  hub_metric_type — reference hub for health metric types.

  Business key: metric_code (e.g. "heart_rate", "spo2", "steps").
  Distinct metric codes are derived from readings as they arrive.
  Record source is 'system_seed' because these codes are effectively
  a reference domain defined by the simulator.
*/

select
    {{ dbt_utils.generate_surrogate_key(['metric_code']) }}  as metric_type_hk,
    metric_code                                               as metric_code_bk,
    load_datetime,
    'system_seed'                                             as record_source

from (
    select distinct
        metric_code,
        min(load_datetime) as load_datetime
    from {{ ref('stg_readings') }}
    where metric_code is not null
    group by metric_code
) distinct_metrics

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
