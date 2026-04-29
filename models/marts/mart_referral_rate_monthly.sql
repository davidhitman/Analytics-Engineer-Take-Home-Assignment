{{ config(materialized='table') }}

-- Extract consultation IDs and normalize timestamps to month start

with base as (

    select
        consultation_id,
        toStartOfMonth(created_at_utc) as month
    from {{ ref('stg_consultations_fixed') }}

),

-- Pull the classified referral types for consultations
classified as (

    select *
    from {{ ref('int_referrals_classified') }}

),

-- Join base consultations and their classified referral types
joined as (

    select
        b.month,
        b.consultation_id,
        c.referral_type

    from base b
    -- Left join ensures all consultations are present, even if not classified
    left join classified c
        on b.consultation_id = c.consultation_id

),

-- Aggregate referral counts for each month
aggregated as (

    select
        month,
        count() as total_consults,

        -- Count consultations with a clinical referral
        countIf(referral_type in ('doctor_referral','both')) as doctor_referrals,
        -- Count consultations with a patient-requested referral
        countIf(referral_type in ('patient_requested_only','both')) as patient_referrals

    from joined
    group by month

)

-- Compute referral rates as a share of total monthly consultations
select
    month,
    total_consults,
    doctor_referrals / total_consults as doctor_referral_rate,
    patient_referrals / total_consults as patient_requested_rate

from aggregated


-- =========================
-- Metric Definition Notes
-- =========================
-- Doctor-issued referral rate includes consultations where a doctor explicitly issued a referral.
-- Cases classified as "both" are included in this metric because a clinical decision was made.
--
-- Patient-requested referral rate includes any consultation where the patient selected the referral
-- request option, regardless of whether a doctor ultimately issued a referral. "Both" cases are also
-- included here to reflect overlap between patient intent and clinician action.
--
-- Patient-requested referrals are not included in the primary clinical referral metric, as they do not
-- represent confirmed clinical decisions. Including them would change the meaning of the metric and
-- make it non-comparable to historical values that reflect clinician behavior only.
