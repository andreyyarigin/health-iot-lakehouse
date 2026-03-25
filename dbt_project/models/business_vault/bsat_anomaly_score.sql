{{
  config(
    materialized = 'incremental',
    unique_key = ['patient_hk', 'report_date'],
    incremental_strategy = 'append',
    schema = 'business_vault'
  )
}}

/*
  bsat_anomaly_score — daily anomaly assessment per patient.

  Computes z-scores for heart rate, SpO2, and steps against each patient's
  own 30-day rolling baseline. A composite anomaly score is the weighted sum:
    composite = hr_zscore * 0.4 + spo2_zscore * 0.4 + steps_zscore * 0.2

  anomaly_flag is TRUE when composite_anomaly_score > 2.0.

  contributing_metrics lists which individual metric z-scores exceeded 1.5,
  formatted as a comma-separated string for human readability.

  z-score formula:  (daily_value - baseline_mean) / baseline_std
  When baseline_std is 0 or NULL (too few data points), z-score defaults to 0.
*/

with vitals_wide as (
    -- Pivot daily aggregates into one row per patient per day for the three
    -- primary anomaly-scoring metrics
    select
        patient_hk,
        report_date,
        max(case when metric_code = 'heart_rate' then value_mean  end) as hr_mean,
        max(case when metric_code = 'spo2'       then value_mean  end) as spo2_mean,
        max(case when metric_code = 'steps'      then value_mean  end) as steps_mean
    from {{ ref('bsat_daily_vitals_agg') }}
    where metric_code in ('heart_rate', 'spo2', 'steps')
    group by patient_hk, report_date
),

baseline as (
    -- 30-day rolling window for each patient — excludes the current day
    -- so the z-score represents deviation from prior history
    select
        curr.patient_hk,
        curr.report_date,
        curr.hr_mean,
        curr.spo2_mean,
        curr.steps_mean,

        -- HR baseline (30-day prior)
        avg(prior.hr_mean)    over w as hr_baseline_mean,
        stddev(prior.hr_mean) over w as hr_baseline_std,

        -- SpO2 baseline (30-day prior)
        avg(prior.spo2_mean)    over w as spo2_baseline_mean,
        stddev(prior.spo2_mean) over w as spo2_baseline_std,

        -- Steps baseline (30-day prior)
        avg(prior.steps_mean)    over w as steps_baseline_mean,
        stddev(prior.steps_mean) over w as steps_baseline_std

    from vitals_wide curr
    -- Self-join to get the 30-day window ending the day before the current row
    join vitals_wide prior
        on  prior.patient_hk  = curr.patient_hk
        and prior.report_date >= date_add('day', -30, curr.report_date)
        and prior.report_date <  curr.report_date

    window w as (
        partition by curr.patient_hk, curr.report_date
    )
),

scored as (
    select
        patient_hk,
        report_date,

        -- HR z-score (absolute — tachycardia and bradycardia both matter)
        case
            when hr_baseline_std is null or hr_baseline_std = 0 then 0.0
            else abs(hr_mean - hr_baseline_mean) / hr_baseline_std
        end as hr_zscore,

        -- SpO2 z-score (directional — low SpO2 is the risk; negate to make
        -- low values produce a positive score)
        case
            when spo2_baseline_std is null or spo2_baseline_std = 0 then 0.0
            else (spo2_baseline_mean - spo2_mean) / spo2_baseline_std
        end as spo2_zscore,

        -- Steps z-score (absolute — unusually low OR high activity matters)
        case
            when steps_baseline_std is null or steps_baseline_std = 0 then 0.0
            else abs(steps_mean - steps_baseline_mean) / steps_baseline_std
        end as steps_zscore,

        hr_mean,
        spo2_mean,
        steps_mean

    from baseline
    -- Deduplicate: the self-join produces one row per (curr, prior) combination;
    -- we want one row per (patient_hk, report_date)
    group by
        patient_hk,
        report_date,
        hr_mean,
        spo2_mean,
        steps_mean,
        hr_baseline_mean, hr_baseline_std,
        spo2_baseline_mean, spo2_baseline_std,
        steps_baseline_mean, steps_baseline_std
),

final as (
    select
        patient_hk,
        report_date,
        current_timestamp                          as load_datetime,
        'business_vault'                           as record_source,

        cast(hr_zscore    as decimal(8, 4))        as hr_zscore,
        cast(spo2_zscore  as decimal(8, 4))        as spo2_zscore,
        cast(steps_zscore as decimal(8, 4))        as steps_zscore,

        cast(
            hr_zscore * 0.4 + spo2_zscore * 0.4 + steps_zscore * 0.2
            as decimal(8, 4)
        )                                           as composite_anomaly_score,

        (hr_zscore * 0.4 + spo2_zscore * 0.4 + steps_zscore * 0.2) > 2.0
                                                    as anomaly_flag,

        -- Human-readable list of metrics that drove the anomaly
        trim(
            concat_ws(
                ', ',
                case when hr_zscore    > 1.5 then 'heart_rate'    else null end,
                case when spo2_zscore  > 1.5 then 'spo2'          else null end,
                case when steps_zscore > 1.5 then 'steps'         else null end
            )
        )                                           as contributing_metrics

    from scored
)

select *
from final

{% if is_incremental() %}
where load_datetime > (select max(load_datetime) from {{ this }})
{% endif %}
