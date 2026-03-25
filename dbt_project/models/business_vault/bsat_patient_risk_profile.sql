{{
  config(
    materialized = 'incremental',
    unique_key = ['patient_hk', 'report_date'],
    incremental_strategy = 'append',
    schema = 'business_vault'
  )
}}

/*
  bsat_patient_risk_profile — patient-level risk profile per day.

  Combines:
    - Demographics and chronic conditions from sat_patient_demographics
    - Recent anomaly score trends from bsat_anomaly_score
    - Data completeness from bsat_daily_vitals_agg

  risk_tier classification:
    low       composite_anomaly_score < 1
    moderate  1 <= composite_anomaly_score < 2
    high      2 <= composite_anomaly_score < 3
    critical  composite_anomaly_score >= 3
*/

with demographics as (
    -- Latest version of demographics per patient
    select
        patient_hk,
        birth_date,
        gender,
        bmi,
        chronic_conditions,
        -- Derive condition flags from SNOMED codes stored in the comma-separated string
        contains(split(chronic_conditions, ','), '44054006')  -- Diabetes mellitus type 2
            or contains(split(chronic_conditions, ','), '15777000')  -- Prediabetes
            or contains(split(chronic_conditions, ','), '368581000119106')
                                                               as has_diabetes,
        contains(split(chronic_conditions, ','), '38341003')   as has_hypertension,
        contains(split(chronic_conditions, ','), '53741008')  -- Coronary artery disease
            or contains(split(chronic_conditions, ','), '413844008')  -- Chronic ischemic
            or contains(split(chronic_conditions, ','), '44784217')   -- Arrhythmia
                                                               as has_cardiac_condition,
        cardinality(
            filter(
                split(chronic_conditions, ','),
                c -> c != ''
            )
        )                                                      as condition_count
    from (
        select
            patient_hk,
            birth_date,
            gender,
            bmi,
            chronic_conditions,
            row_number() over (
                partition by patient_hk
                order by effective_from desc
            ) as rn
        from {{ ref('sat_patient_demographics') }}
    ) versioned
    where rn = 1
),

anomaly_scores as (
    select
        patient_hk,
        report_date,
        composite_anomaly_score,
        anomaly_flag
    from {{ ref('bsat_anomaly_score') }}
),

rolling_anomaly as (
    select
        a.patient_hk,
        a.report_date,
        a.composite_anomaly_score,
        a.anomaly_flag,

        -- 7-day rolling average of composite score
        avg(a.composite_anomaly_score) over (
            partition by a.patient_hk
            order by a.report_date
            rows between 6 preceding and current row
        )                                                      as avg_anomaly_score_7d,

        -- Count of anomaly days in last 30 days (inclusive of current day)
        sum(cast(a.anomaly_flag as integer)) over (
            partition by a.patient_hk
            order by a.report_date
            rows between 29 preceding and current row
        )                                                      as anomaly_events_30d

    from anomaly_scores a
),

completeness_7d as (
    -- Average data completeness across all metrics for the last 7 days
    select
        v.patient_hk,
        v.report_date,
        avg(v.completeness_pct)                                as data_completeness_7d
    from {{ ref('bsat_daily_vitals_agg') }} v
    join anomaly_scores a
        on  a.patient_hk  = v.patient_hk
        and v.report_date >= date_add('day', -6, a.report_date)
        and v.report_date <= a.report_date
    group by v.patient_hk, v.report_date
)

select
    ra.patient_hk,
    ra.report_date,
    current_timestamp                                          as load_datetime,
    'business_vault'                                           as record_source,

    -- Age in full years as of report_date
    date_diff('year', d.birth_date, ra.report_date)            as age,

    d.condition_count,
    d.has_diabetes,
    d.has_hypertension,
    d.has_cardiac_condition,

    cast(ra.avg_anomaly_score_7d  as decimal(8, 4))            as avg_anomaly_score_7d,
    cast(ra.anomaly_events_30d    as integer)                   as anomaly_events_30d,
    cast(c.data_completeness_7d   as decimal(5, 2))            as data_completeness_7d,

    case
        when ra.composite_anomaly_score < 1 then 'low'
        when ra.composite_anomaly_score < 2 then 'moderate'
        when ra.composite_anomaly_score < 3 then 'high'
        else 'critical'
    end                                                        as risk_tier

from rolling_anomaly ra
join demographics d
    on d.patient_hk = ra.patient_hk
left join completeness_7d c
    on  c.patient_hk  = ra.patient_hk
    and c.report_date = ra.report_date

{% if is_incremental() %}
where ra.report_date > (
    select date_add('day', -1, max(report_date)) from {{ this }}
)
{% endif %}
