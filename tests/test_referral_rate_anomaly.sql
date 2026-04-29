-- This test is designed to catch anomaly spikes in the monthly doctor referral rate.
-- It compares each month's referral rate to the previous month's and flags cases where
-- the referral rate has increased by more than 50% from one month to the next.

with rates as (

    select
        month, -- The time period being analyzed
        doctor_referral_rate, -- Referral rate for the current month
        lag(doctor_referral_rate) over (order by month) as prev_rate -- Previous month's rate for comparison
    from {{ ref('mart_referral_rate_monthly') }}

)

select *
from rates
where prev_rate is not null  -- Only consider months which have a previous month for comparison
  and doctor_referral_rate > prev_rate * 1.5 -- Flag as anomaly if rate jumps > 50%
