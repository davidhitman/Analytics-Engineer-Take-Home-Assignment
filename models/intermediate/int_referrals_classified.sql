{{ config(materialized='view') }}

-- Create 'joined' to combine referral signals from consultations, outcomes, and requests

with joined as (

    select
        c.consultation_id,

        -- Use 0 if 'referral_issued' is NULL
        ifNull(o.referral_issued, 0) as doctor_referral,
        -- Use 0 if 'referral_requested' is NULL
        ifNull(i.referral_requested, 0) as patient_requested

    from {{ ref('stg_consultations_fixed') }} c
    -- Join to clinical outcomes to get doctor-issued referral information
    left join {{ ref('stg_clinical_outcomes') }} o
        on c.consultation_id = o.consultation_id
    -- Join to consultation requests to get patient-requested referral information
    left join {{ ref('stg_consultation_requests') }} i
        on c.consultation_id = i.consultation_id

)
    
-- classify each consultation based on the referral signals
select
    consultation_id,

    -- Assign a referral_type based on the presence or combination of each referral signal
    case
        when doctor_referral = 1 and patient_requested = 1 then 'both'  -- Both doctor and patient triggered a referral on this consultation
        when doctor_referral = 1 then 'doctor_referral' -- Only the doctor triggered a referral
        when patient_requested = 1 then 'patient_requested_only' -- Only the patient requested a referral
        else 'no_referral' -- No referral activity detected
    end as referral_type

from joined
