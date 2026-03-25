{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  mart_anomaly_dashboard — aggregated BI view with current patient status.

  One row per patient. Shows the most recent vitals, anomaly score, risk tier,
  and alert count. Designed for consumption by Superset or similar BI tools.

  Columns follow the specification in docs/DATA_VAULT_MODEL.md:
  mart_anomaly_dashboard.
*/

with latest_demographics as (
    -- Most recent version of demographics per patient
    select
        pd.patient_hk,
        hp.patient_bk,
        pd.gender,
        pd.birth_date,
        date_diff('year', pd.birth_date, current_date) as age,
        pd.chronic_conditions
    from {{ ref('sat_patient_demographics') }} pd
    join {{ ref('hub_patient') }} hp
        on hp.patient_hk = pd.patient_hk
    where pd.effective_from = (
        select max(sub.effective_from)
        from {{ ref('sat_patient_demographics') }} sub
        where sub.patient_hk = pd.patient_hk
    )
),

latest_risk as (
    -- Most recent risk profile per patient
    select
        patient_hk,
        report_date,
        risk_tier,
        avg_anomaly_score_7d,
        anomaly_events_30d
    from (
        select
            patient_hk,
            report_date,
            risk_tier,
            avg_anomaly_score_7d,
            anomaly_events_30d,
            row_number() over (
                partition by patient_hk
                order by report_date desc
            ) as rn
        from {{ ref('bsat_patient_risk_profile') }}
    ) ranked
    where rn = 1
),

latest_anomaly as (
    -- Today's (most recent) anomaly score per patient
    select
        patient_hk,
        report_date,
        composite_anomaly_score as anomaly_score_today,
        anomaly_flag
    from (
        select
            patient_hk,
            report_date,
            composite_anomaly_score,
            anomaly_flag,
            row_number() over (
                partition by patient_hk
                order by report_date desc
            ) as rn
        from {{ ref('bsat_anomaly_score') }}
    ) ranked
    where rn = 1
),

latest_hr as (
    -- Most recent heart rate reading per patient
    select
        patient_hk,
        value as latest_hr
    from (
        select
            patient_hk,
            value,
            row_number() over (
                partition by patient_hk
                order by measured_at desc
            ) as rn
        from {{ ref('sat_reading_value') }}
        where metric_code = 'heart_rate'
    ) ranked
    where rn = 1
),

latest_spo2 as (
    -- Most recent SpO2 reading per patient
    select
        patient_hk,
        value as latest_spo2
    from (
        select
            patient_hk,
            value,
            row_number() over (
                partition by patient_hk
                order by measured_at desc
            ) as rn
        from {{ ref('sat_reading_value') }}
        where metric_code = 'spo2'
    ) ranked
    where rn = 1
),

days_since_anomaly as (
    -- Days since most recent anomaly event per patient
    select
        patient_hk,
        date_diff(
            'day',
            max(report_date),
            current_date
        )                        as days_since_last_anomaly
    from {{ ref('bsat_anomaly_score') }}
    where anomaly_flag = true
    group by patient_hk
),

active_alerts as (
    -- Count of unacknowledged alerts per patient
    select
        rv.patient_hk,
        count(*) as active_alerts
    from {{ ref('sat_alert_detail') }} ad
    join {{ ref('hub_alert') }} ha
        on ha.alert_hk = ad.alert_hk
    join {{ ref('lnk_reading_alert') }} lra
        on lra.alert_hk = ad.alert_hk
    join {{ ref('sat_reading_value') }} rv
        on rv.reading_hk = lra.reading_hk
    where ad.acknowledged = false
    group by rv.patient_hk
),

anomaly_trend as (
    -- Compare last 3 days vs previous 3 days to derive improving/stable/worsening
    select
        patient_hk,
        avg(case
                when report_date >= date_add('day', -3, current_date)
                then composite_anomaly_score
            end) as recent_avg,
        avg(case
                when report_date >= date_add('day', -6, current_date)
                 and report_date  < date_add('day', -3, current_date)
                then composite_anomaly_score
            end) as prior_avg
    from {{ ref('bsat_anomaly_score') }}
    where report_date >= date_add('day', -6, current_date)
    group by patient_hk
)

select
    d.patient_bk,

    -- Derive patient_name from Synthea first/last names stored in hub patient BK
    -- (actual names come from staging; we use patient_bk as identifier here)
    cast(d.patient_bk  as varchar)                    as patient_name,

    d.age,
    r.risk_tier,

    cast(hr.latest_hr   as decimal(8, 2))              as latest_hr,
    cast(s.latest_spo2  as decimal(8, 2))              as latest_spo2,

    cast(a.anomaly_score_today as decimal(8, 4))       as anomaly_score_today,

    case
        when t.recent_avg is null
          or t.prior_avg  is null  then 'stable'
        when t.recent_avg < t.prior_avg * 0.9            then 'improving'
        when t.recent_avg > t.prior_avg * 1.1            then 'worsening'
        else 'stable'
    end                                                as anomaly_trend,

    coalesce(ds.days_since_last_anomaly, null)         as days_since_last_anomaly,
    coalesce(aa.active_alerts, 0)                      as active_alerts

from latest_demographics d
join latest_risk r
    on r.patient_hk = d.patient_hk
join latest_anomaly a
    on a.patient_hk = d.patient_hk
left join latest_hr hr
    on hr.patient_hk = d.patient_hk
left join latest_spo2 s
    on s.patient_hk = d.patient_hk
left join days_since_anomaly ds
    on ds.patient_hk = d.patient_hk
left join active_alerts aa
    on aa.patient_hk = d.patient_hk
left join anomaly_trend t
    on t.patient_hk = d.patient_hk
