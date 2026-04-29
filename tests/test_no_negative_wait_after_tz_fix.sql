-- This test ensures that after applying timezone corrections,
-- there are no consultations with negative wait times.
-- It checks the 'stg_consultations_fixed' table for records flagged as timezone-corrected,
-- and asserts that none have a negative 'wait_time_minutes' value.

select *
from {{ ref('stg_consultations_fixed') }}
where is_tz_corrected = 1 -- Only check records where timezone correction was applied
  and wait_time_minutes < 0 -- Fail the test if any wait time is negative
