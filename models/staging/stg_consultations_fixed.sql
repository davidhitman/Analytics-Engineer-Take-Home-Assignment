{{ config(materialized='view') }}

with source_data as (

    select *
    from {{ source('raw', 'consultations') }}

),

parsed as (

    select
        consultation_id,
        patient_id,

        parseDateTimeBestEffort(created_at) as created_at_utc,
        parseDateTimeBestEffort(consultation_started_at) as started_raw,

        consultation_started_at

    from source_data
    where patient_id not like 'TEST_%'

),

normalized as (

    select
        consultation_id,
        patient_id,
        created_at_utc,

        -- Normalize all timestamps to UTC
        toTimeZone(started_raw, 'UTC') as started_at_utc,

        -- Flag potential timezone mismatch cases
        case
            when consultation_started_at like '%+2%' then 1
            else 0
        end as is_tz_corrected

    from parsed

),

final as (

    select
        consultation_id,
        patient_id,
        created_at_utc,
        started_at_utc,
        is_tz_corrected,

        dateDiff('minute', created_at_utc, started_at_utc) as raw_wait_time,

        -- Clean wait time
        case
            when dateDiff('minute', created_at_utc, started_at_utc) < 0 then null
            when dateDiff('minute', created_at_utc, started_at_utc) > 360 then null  -- If the wait time is above 6 hours
            else dateDiff('minute', created_at_utc, started_at_utc)
        end as wait_time_minutes

    from normalized

)

select * from final
