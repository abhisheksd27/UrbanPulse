-- ==============================================================================
-- Model: stg_weather
-- Layer: Staging
-- Materialization: View
-- Schema: staging
--
-- WHAT THIS MODEL DOES:
-- Casts, validates, and enriches hourly weather observations from Open-Meteo.
-- Joins with the WMO weather codes seed table for descriptive labels.
-- ==============================================================================

{{ config(materialized='view') }}

with source_data as (

    select
        cast(time as timestamp)                                as observed_at,
        cast(temperature_2m as decimal(5,2))                   as temperature_c,
        cast(coalesce(precipitation, 0.0) as decimal(6,2))     as precipitation_mm,
        cast(coalesce(windspeed_10m, 0.0) as decimal(6,2))     as wind_speed_kmh,
        cast(coalesce(visibility, 10000) as integer)           as visibility_m,
        cast(coalesce(weathercode, 0) as integer)              as weather_code

    from {{ source('landing', 'weather_observations') }}

    where time is not null

),

enriched as (

    select
        w.observed_at,
        cast(w.observed_at as date)                            as observed_date,
        extract(hour from w.observed_at)                       as observed_hour,
        w.temperature_c,
        -- Temperature in Fahrenheit for US operational context
        round((w.temperature_c * 9.0 / 5.0) + 32.0, 2)         as temperature_f,
        w.precipitation_mm,
        w.wind_speed_kmh,
        w.visibility_m,
        w.weather_code,
        coalesce(c.description, 'Unknown')                     as weather_description,
        coalesce(c.severity, 'None')                           as weather_severity,
        case when w.precipitation_mm > 0.1 then true else false end as is_raining,
        case when w.weather_code = 0 then true else false end       as is_clear,
        case when w.weather_code in (45, 48) then true else false end as is_foggy

    from source_data w
    left join {{ ref('wmo_weather_codes') }} c
        on w.weather_code = c.code

)

select * from enriched
