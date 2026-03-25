{{
  config(
    materialized = 'table',
    schema = 'marts'
  )
}}

/*
  mart_patient_daily_features — THE ML feature table.

  One row per patient per day. Contains all features needed for anomaly
  prediction, including:
    - Static demographics (age, gender, BMI, chronic conditions)
    - Daily vital sign statistics (mean, std, min, max, percentiles)
    - 7-day rolling averages for trend features
    - Day-over-day delta for HR (short-term change)
    - anomaly_next_24h: the ML TARGET — TRUE if tomorrow has anomaly_flag=TRUE

  Full refresh on every dbt run (table materialization). This ensures the
  anomaly_next_24h label is always consistent with the latest anomaly data.

  Columns follow the specification in docs/DATA_VAULT_MODEL.md:
  mart_patient_daily_features.
*/

with vitals as (
    -- Pivot daily vital aggregates into wide format (one row per patient per day)
    select
        patient_hk,
        report_date,

        -- Heart rate
        max(case when metric_code = 'heart_rate' then value_mean  end) as hr_mean,
        max(case when metric_code = 'heart_rate' then value_std   end) as hr_std,
        max(case when metric_code = 'heart_rate' then value_min   end) as hr_min,
        max(case when metric_code = 'heart_rate' then value_max   end) as hr_max,
        max(case when metric_code = 'heart_rate' then value_p05   end) as hr_p05,
        max(case when metric_code = 'heart_rate' then value_p95   end) as hr_p95,

        -- SpO2
        max(case when metric_code = 'spo2'       then value_mean  end) as spo2_mean,
        max(case when metric_code = 'spo2'       then value_min   end) as spo2_min,

        -- Steps
        max(case when metric_code = 'steps'      then reading_count * value_mean
                 end)                                                   as steps_total,

        -- Skin temperature
        max(case when metric_code = 'skin_temperature' then value_mean end) as skin_temp_mean,

        -- Blood glucose (nullable — diabetics only)
        max(case when metric_code = 'blood_glucose' then value_mean end) as glucose_mean,
        max(case when metric_code = 'blood_glucose' then value_std  end) as glucose_std,

        -- Sleep (approximate hours from sleep_stage reading count × 1-min epochs)
        max(case when metric_code = 'sleep_stage'
                 then reading_count / 60.0  -- readings are ~1/min, so count ≈ minutes
                 end)                                                   as sleep_duration_hrs,

        -- Data completeness: average across all metrics for that day
        avg(completeness_pct)                                           as data_completeness

    from {{ ref('bsat_daily_vitals_agg') }}
    group by patient_hk, report_date
),

rolling_vitals as (
    -- 7-day rolling averages and day-over-day deltas
    select
        patient_hk,
        report_date,
        hr_mean,
        hr_std,
        hr_min,
        hr_max,
        hr_p05,
        hr_p95,
        spo2_mean,
        spo2_min,
        steps_total,
        skin_temp_mean,
        glucose_mean,
        glucose_std,
        sleep_duration_hrs,
        data_completeness,

        -- 7-day rolling averages
        avg(hr_mean) over (
            partition by patient_hk
            order by report_date
            rows between 6 preceding and current row
        )                                                as hr_mean_7d_avg,

        avg(spo2_min) over (
            partition by patient_hk
            order by report_date
            rows between 6 preceding and current row
        )                                                as spo2_min_7d_avg,

        avg(steps_total) over (
            partition by patient_hk
            order by report_date
            rows between 6 preceding and current row
        )                                                as steps_total_7d_avg,

        -- Day-over-day delta for HR
        hr_mean - lag(hr_mean) over (
            partition by patient_hk
            order by report_date
        )                                                as hr_mean_delta_1d

    from vitals
),

anomaly as (
    select patient_hk, report_date, composite_anomaly_score, anomaly_flag
    from {{ ref('bsat_anomaly_score') }}
),

risk as (
    select
        patient_hk,
        report_date,
        age,
        condition_count,
        has_diabetes,
        has_hypertension,
        has_cardiac_condition,
        anomaly_events_30d,
        risk_tier
    from {{ ref('bsat_patient_risk_profile') }}
),

demographics as (
    -- Latest version of demographics per patient
    select
        patient_hk,
        gender,
        bmi
    from (
        select
            patient_hk,
            gender,
            bmi,
            row_number() over (
                partition by patient_hk
                order by effective_from desc
            ) as rn
        from {{ ref('sat_patient_demographics') }}
    ) versioned
    where rn = 1
),

-- Self-join to get tomorrow's anomaly_flag as the ML target label
anomaly_tomorrow as (
    select
        patient_hk,
        date_add('day', -1, report_date) as feature_date,
        anomaly_flag                      as anomaly_next_24h
    from anomaly
)

select
    rv.patient_hk,
    rv.report_date,

    -- Demographics
    r.age,
    d.gender,
    cast(d.bmi                  as decimal(5, 2))   as bmi,
    r.has_diabetes,
    r.has_hypertension,
    r.has_cardiac_condition,
    r.condition_count,

    -- Daily vital statistics
    cast(rv.hr_mean             as decimal(8, 4))   as hr_mean,
    cast(rv.hr_std              as decimal(8, 4))   as hr_std,
    cast(rv.hr_min              as decimal(8, 4))   as hr_min,
    cast(rv.hr_max              as decimal(8, 4))   as hr_max,
    cast(rv.hr_p05              as decimal(8, 4))   as hr_p05,
    cast(rv.hr_p95              as decimal(8, 4))   as hr_p95,
    cast(rv.spo2_mean           as decimal(8, 4))   as spo2_mean,
    cast(rv.spo2_min            as decimal(8, 4))   as spo2_min,
    cast(rv.steps_total         as integer)         as steps_total,
    cast(rv.skin_temp_mean      as decimal(8, 4))   as skin_temp_mean,
    cast(rv.glucose_mean        as decimal(8, 4))   as glucose_mean,
    cast(rv.glucose_std         as decimal(8, 4))   as glucose_std,
    cast(rv.sleep_duration_hrs  as decimal(6, 2))   as sleep_duration_hrs,

    -- Rolling / trend features
    cast(rv.hr_mean_7d_avg      as decimal(8, 4))   as hr_mean_7d_avg,
    cast(rv.hr_mean_delta_1d    as decimal(8, 4))   as hr_mean_delta_1d,
    cast(rv.spo2_min_7d_avg     as decimal(8, 4))   as spo2_min_7d_avg,
    cast(rv.steps_total_7d_avg  as decimal(10, 2))  as steps_total_7d_avg,

    -- Anomaly score
    cast(a.composite_anomaly_score as decimal(8, 4)) as composite_anomaly_score,
    r.anomaly_events_30d,
    cast(rv.data_completeness   as decimal(5, 2))   as data_completeness,
    r.risk_tier,

    -- ML target: does this patient have an anomaly tomorrow?
    coalesce(at.anomaly_next_24h, false)             as anomaly_next_24h

from rolling_vitals rv
join demographics d
    on d.patient_hk  = rv.patient_hk
join risk r
    on  r.patient_hk  = rv.patient_hk
    and r.report_date = rv.report_date
join anomaly a
    on  a.patient_hk  = rv.patient_hk
    and a.report_date = rv.report_date
left join anomaly_tomorrow at
    on  at.patient_hk  = rv.patient_hk
    and at.feature_date = rv.report_date
