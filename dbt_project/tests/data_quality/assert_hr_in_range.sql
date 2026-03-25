/*
  assert_hr_in_range — fail if any heart rate reading is outside the
  physiologically plausible range of 20–300 bpm.

  A result set with rows = TEST FAILURE (dbt convention: failing tests return rows).
  An empty result set = PASS.

  The range is deliberately wide to catch only implausible sensor errors,
  not clinical abnormalities (those are handled by the alert generator).
*/

select
    reading_hk,
    patient_hk,
    measured_at,
    value              as heart_rate_value,
    quality_flag,
    load_datetime

from {{ ref('sat_reading_value') }}

where metric_code = 'heart_rate'
  and (value < 20 or value > 300)
