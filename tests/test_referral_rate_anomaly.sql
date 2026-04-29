with rates as (

    select
        month,
        doctor_referral_rate,
        lag(doctor_referral_rate) over (order by month) as prev_rate
    from {{ ref('mart_referral_rate_monthly') }}

)

select *
from rates
where prev_rate is not null
  and doctor_referral_rate > prev_rate * 1.5
