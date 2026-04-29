{{ config(materialized='view') }}

with joined as (

    select
        c.consultation_id,

        ifNull(o.referral_issued, 0) as doctor_referral,
        ifNull(i.referral_requested, 0) as patient_requested

    from {{ ref('stg_consultations_fixed') }} c
    left join {{ ref('stg_clinical_outcomes') }} o
        on c.consultation_id = o.consultation_id
    left join {{ ref('stg_consultation_requests') }} i
        on c.consultation_id = i.consultation_id

)

select
    consultation_id,

    case
        when doctor_referral = 1 and patient_requested = 1 then 'both'
        when doctor_referral = 1 then 'doctor_referral'
        when patient_requested = 1 then 'patient_requested_only'
        else 'no_referral'
    end as referral_type

from joined
