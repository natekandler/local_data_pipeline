{{ config(
    materialized='view',
    meta={
        "dagster": {
            "group": "swell_data_daily_max"
        }
    }
) }}

with ranked as (
    select
        *,
        row_number() over (
            partition by dt, location
            order by swell_wave_height desc, timestamp desc
        ) as rn
    from {{ ref('stg_open_meteo__swell_data_s3') }}
)
select
    timestamp,
    location,
    wave_height,
    wave_direction, 
    wind_wave_direction,
    swell_wave_height,
    swell_wave_direction,
    swell_wave_period,
    dt,
    retrieved_at
from ranked
where rn = 1

