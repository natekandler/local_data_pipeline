-- Reference dbt model (Spark/Databricks SQL compatible)
-- Transforms raw Open-Meteo JSON payloads into hourly wave metrics
--
-- Expected raw table schema:
--   - timestamp: timestamp (ingest time)
--   - location: string (e.g., 'Tamarack')
--   - data: string (JSON of the Open-Meteo response)
--
-- How to use in dbt:
-- 1) Define a source pointing at your Delta table/location.
--    Example (sources.yml):
--
--    sources:
--      - name: raw
--        tables:
--          - name: open_meteo
--            meta:
--              location: "s3://{{ env_var('RAW_BUCKET') }}/{{ env_var('RAW_PREFIX') }}"
--              format: delta
--
--    Your adapter must support reading Delta at an S3 path (e.g., dbt-spark, dbt-databricks).
--    Alternatively, register the Delta path as a table in your metastore and reference it directly.
--
-- 2) Create this model file and reference the source below.
-- 3) Run: dbt run -s wave_data_processed

{{
  config(
    materialized='table',
    file_format='delta',
    partition_by=['dt', 'location']
  )
}}

with raw as (
  select
    cast(timestamp as timestamp) as ingest_ts,
    location,
    -- Parse JSON payload into a typed struct for easy access
    from_json(
      data,
      'struct<
         latitude:double,
         longitude:double,
         generationtime_ms:double,
         utc_offset_seconds:int,
         timezone:string,
         timezone_abbreviation:string,
         elevation:double,
         hourly_units:map<string,string>,
         hourly:struct<
           time:array<string>,
           wave_height:array<double>,
           wave_direction:array<double>,
           wind_wave_direction:array<double>,
           swell_wave_height:array<double>,
           swell_wave_direction:array<double>,
           swell_wave_period:array<double>
         >
       >'
    ) as j
  from {{ source('raw', 'open_meteo') }}
),
exploded as (
  -- Zip all the hourly arrays into an array of structs, then explode to rows
  select
    r.location,
    h.time as time_str,
    h.wave_height as wave_height,
    h.wave_direction as wave_direction,
    h.wind_wave_direction as wind_wave_direction,
    h.swell_wave_height as swell_wave_height,
    h.swell_wave_direction as swell_wave_direction,
    h.swell_wave_period as swell_wave_period
  from raw r
  lateral view explode(arrays_zip(
    r.j.hourly.time,
    r.j.hourly.wave_height,
    r.j.hourly.wave_direction,
    r.j.hourly.wind_wave_direction,
    r.j.hourly.swell_wave_height,
    r.j.hourly.swell_wave_direction,
    r.j.hourly.swell_wave_period
  )) z as h
)
select
  to_timestamp(time_str) as timestamp,
  location,
  cast(wave_height as double) as wave_height,
  cast(wave_direction as double) as wave_direction,
  cast(wind_wave_direction as double) as wind_wave_direction,
  cast(swell_wave_height as double) as swell_wave_height,
  cast(swell_wave_direction as double) as swell_wave_direction,
  cast(swell_wave_period as double) as swell_wave_period,
  date(to_timestamp(time_str)) as dt
from exploded


