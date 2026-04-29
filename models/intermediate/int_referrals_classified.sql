{{ config(materialized='view') }}

with consultations as (

    select *
    from {{ ref('stg_consultations_fixed') }}

),

outcomes as (

    select *
    from {{ ref('stg_clinical_outcomes') }}

),

intake as (

    select *
    from {{ ref('stg_consultation_requests') }}

),

joined as (

    select
        c.consultation_id,

        coalesce(o.referral_issued, 0) as doctor_referral,
        coalesce(i.referral_requested, 0) as patient_requested

    from consultations c
    left join outcomes o
        on c.consultation_id = o.consultation_id
    left join intake i
        on c.consultation_id = i.consultation_id

),

classified as (

    select
        consultation_id,

        case
            when doctor_referral = 1 and patient_requested = 1 then 'both'
            when doctor_referral = 1 then 'doctor_referral'
            when patient_requested = 1 then 'patient_requested_only'
            else 'no_referral'
        end as referral_type

    from joined

)

select * from classified
