/*
  assert_spo2_in_range — fail if any SpO2 reading is outside the
  physiologically plausible range of 70–100 %.

  Values below 70 % are incompatible with life under sustained conditions
  and indicate sensor error in a synthetic dataset.
  Values above 100 % are physically impossible.

  A result set with rows = TEST FAILURE.
  An empty result set = PASS.
*/

select
    reading_hk,
    patient_hk,
    measured_at,
    value              as spo2_value,
    quality_flag,
    load_datetime

from {{ ref('sat_reading_value') }}

where metric_code = 'spo2'
  and (value < 70 or value > 100)
