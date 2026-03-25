{{
  config(
    materialized = 'incremental',
    unique_key = 'reading_alert_hk',
    incremental_strategy = 'append',
    schema = 'raw_vault'
  )
}}

/*
  lnk_reading_alert — connects the worst reading in a breach window to the alert it triggered.

  The stg_alerts model carries reading_id (the UUID of the worst reading within
  the breach window). Not every reading generates an alert, so this link is sparse.
*/

select
    {{ dbt_utils.generate_surrogate_key(['reading_id', 'alert_id']) }}  as reading_alert_hk,
    {{ dbt_utils.generate_surrogate_key(['reading_id']) }}               as reading_hk,
    {{ dbt_utils.generate_surrogate_key(['alert_id']) }}                 as alert_hk,
    load_datetime,
    record_source

from {{ ref('stg_alerts') }}

where reading_id is not null
  and alert_id   is not null

{% if is_incremental() %}
  and load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
