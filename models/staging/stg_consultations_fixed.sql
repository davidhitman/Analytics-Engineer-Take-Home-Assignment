{{ config(materialized='view') }}

with source_data as (

    select *
    from {{ source('raw', 'consultations') }}

),

cleaned as (

    select
        consultation_id,
        patient_id,

        created_at,

        -- Detect UTC+2 timestamps (string contains offset)
        consultation_started_at,

        -- Normalize timezone: convert all to UTC
        case
            when consultation_started_at like '%+2'
                then toTimeZone(parseDateTimeBestEffort(consultation_started_at), 'UTC')
            else parseDateTimeBestEffort(consultation_started_at)
        end as started_at_utc,

        -- created_at assumed already UTC
        parseDateTimeBestEffort(created_at) as created_at_utc,

        -- Flag if correction applied
        case
            when consultation_started_at like '%+2' then 1
            else 0
        end as is_tz_corrected

    from source_data

    -- Remove test accounts
    where patient_id not like 'TEST_%'

),

final as (

    select
        consultation_id,
        patient_id,
        created_at_utc,
        started_at_utc,
        is_tz_corrected,

        -- Wait time in minutes
        dateDiff('minute', created_at_utc, started_at_utc) as raw_wait_time,

        -- Clean wait time
        case
            -- Remove negative values or unrealistic values (> 6 hours = 360 mins)
            when dateDiff('minute', created_at_utc, started_at_utc) < 0 then null
            when dateDiff('minute', created_at_utc, started_at_utc) > 360 then null
            else dateDiff('minute', created_at_utc, started_at_utc)
        end as wait_time_minutes

    from cleaned

)

select * from final
