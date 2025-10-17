{{ config(materialized='table') }}

select * from {{ ref('stg_wave_open_meteo') }}


