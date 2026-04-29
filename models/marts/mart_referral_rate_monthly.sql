{{ config(materialized='table') }}

with base as (

    select
        consultation_id,
        toStartOfMonth(created_at_utc) as month
    from {{ ref('stg_consultations_fixed') }}

),

classified as (

    select *
    from {{ ref('int_referrals_classified') }}

),

joined as (

    select
        b.month,
        b.consultation_id,
        c.referral_type

    from base b
    left join classified c
        on b.consultation_id = c.consultation_id

),

aggregated as (

    select
        month,
        count() as total_consults,

        countIf(referral_type in ('doctor_referral','both')) as doctor_referrals,
        countIf(referral_type in ('patient_requested_only','both')) as patient_referrals

    from joined
    group by month

)

select
    month,
    total_consults,
    doctor_referrals / total_consults as doctor_referral_rate,
    patient_referrals / total_consults as patient_requested_rate

from aggregated
