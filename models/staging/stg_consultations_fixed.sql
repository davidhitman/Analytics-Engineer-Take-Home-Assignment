{{ config(materialized='view') }}
  
-- Extract the source consultations data from the raw table

with source_data as (

    select *
    from {{ source('raw', 'consultations') }}

),

-- Parse datetime fields and filter out test patient records
parsed as (

    select
        consultation_id,
        patient_id,

        -- Convert created_at to a best-effort UTC datetime
        parseDateTimeBestEffort(created_at) as created_at_utc,
    
        -- Convert consultation_started_at in a similar fashion
        parseDateTimeBestEffort(consultation_started_at) as started_raw,

        -- Keep original raw timestamp (for pattern matching / validation)
        consultation_started_at

    from source_data
    -- Exclude test patients by patient_id pattern
    where patient_id not like 'TEST_%'

),

-- Normalize timestamps and create timezone correction flag
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
    
-- Calculate wait times and clean data
final as (

    select
        consultation_id,
        patient_id,
        created_at_utc,
        started_at_utc,
        is_tz_corrected,

        -- Raw wait time in minutes between creation and appointment start
        dateDiff('minute', created_at_utc, started_at_utc) as raw_wait_time,

        -- Clean wait time
        --   - Null if negative (corrupted data)
        --   - Null if wait time is more than 6 hours (likely erroneous)
        --   - Otherwise, the computed wait time in minutes
        case
            when dateDiff('minute', created_at_utc, started_at_utc) < 0 then null
            when dateDiff('minute', created_at_utc, started_at_utc) > 360 then null  -- If the wait time is above 6 hours
            else dateDiff('minute', created_at_utc, started_at_utc)
        end as wait_time_minutes

    from normalized

)

-- Output the final data for downstream use
select * from final
