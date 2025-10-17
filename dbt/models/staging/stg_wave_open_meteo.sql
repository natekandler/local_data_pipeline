-- Staging model: parses and explodes raw Open-Meteo JSON payloads into hourly rows
-- Intended to feed intermediate/presentation layers

{{ config(materialized='view') }}

with raw as (
  select
    location,
    json(data) as j
  from {{ source('raw', 'open_meteo') }}
),
arrays as (
  select
    location,
    json_extract(j, '$.hourly.time')                 as times_json,
    json_extract(j, '$.hourly.wave_height')          as wh_json,
    json_extract(j, '$.hourly.wave_direction')       as wd_json,
    json_extract(j, '$.hourly.wind_wave_direction')  as wwd_json,
    json_extract(j, '$.hourly.swell_wave_height')    as swh_json,
    json_extract(j, '$.hourly.swell_wave_direction') as swd_json,
    json_extract(j, '$.hourly.swell_wave_period')    as swp_json,
    cast(json_array_length(json_extract(j, '$.hourly.time')) as bigint) as n
  from raw
),
exploded as (
  select
    a.location,
    json_extract_string(a.times_json, printf('$[%d]', i)) as time_str,
    cast(json_extract(a.wh_json,   printf('$[%d]', i)) as double)   as wave_height,
    cast(json_extract(a.wd_json,   printf('$[%d]', i)) as double)   as wave_direction,
    cast(json_extract(a.wwd_json,  printf('$[%d]', i)) as double)   as wind_wave_direction,
    cast(json_extract(a.swh_json,  printf('$[%d]', i)) as double)   as swell_wave_height,
    cast(json_extract(a.swd_json,  printf('$[%d]', i)) as double)   as swell_wave_direction,
    cast(json_extract(a.swp_json,  printf('$[%d]', i)) as double)   as swell_wave_period
  from arrays a,
  generate_series(cast(0 as bigint), cast(a.n - 1 as bigint)) as gs(i)
)
select
  strptime(time_str, '%Y-%m-%dT%H:%M') as timestamp,
  location,
  wave_height,
  wave_direction,
  wind_wave_direction,
  swell_wave_height,
  swell_wave_direction,
  swell_wave_period,
  cast(strptime(time_str, '%Y-%m-%dT%H:%M') as date) as dt
from exploded


