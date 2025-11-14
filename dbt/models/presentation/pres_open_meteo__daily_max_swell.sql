  {{ config(
      materialized='table',
      meta={
          "dagster": {
              "group": "swell_data_daily_max"
          }
      }
  ) }}

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
from {{ ref('int_open_meteo__max_swell_per_day') }}

