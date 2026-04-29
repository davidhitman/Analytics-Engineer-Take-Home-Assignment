{{ config(materialized='table') }}

with base as (

    select
        c.consultation_id,
        toStartOfMonth(c.created_at_utc) as month,
        r.referral_type

    from {{ ref('stg_consultations_fixed') }} c
    left join {{ ref('int_referrals_classified') }} r
        on c.consultation_id = r.consultation_id

),

aggregated as (

    select
        month,
        count(*) as total_consults,

        countIf(referral_type in ('doctor_referral', 'both')) as doctor_referrals,
        countIf(referral_type in ('patient_requested_only', 'both')) as patient_requested

    from base
    group by month

)

select
    month,
    total_consults,

    doctor_referrals / total_consults as doctor_referral_rate,
    patient_requested / total_consults as patient_requested_rate

from aggregated
