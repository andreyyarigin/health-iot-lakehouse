/*
  assert_device_freshness — fail if any known device has sent no readings
  in the last 25 hours.

  This catches devices that are silently failing (battery dead, connectivity
  issue, or simulator not run for that patient).

  25 hours gives a 1-hour buffer beyond the expected 24-hour daily batch
  cadence before raising a test failure.

  A result set with rows = TEST FAILURE.
  An empty result set = PASS (all devices are fresh).
*/

with device_last_reading as (
    select
        device_hk,
        max(measured_at) as last_seen_at
    from {{ ref('sat_reading_value') }}
    group by device_hk
),

stale_devices as (
    select
        d.device_hk,
        d.device_serial_bk,
        dlr.last_seen_at,
        date_diff(
            'hour',
            dlr.last_seen_at,
            current_timestamp
        )                                    as hours_since_last_reading
    from {{ ref('hub_device') }} d
    join device_last_reading dlr
        on dlr.device_hk = d.device_hk
    where date_diff('hour', dlr.last_seen_at, current_timestamp) > 25
)

select *
from stale_devices
