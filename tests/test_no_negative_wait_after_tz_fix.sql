select *
from {{ ref('stg_consultations_fixed') }}
where is_tz_corrected = 1
  and wait_time_minutes < 0
