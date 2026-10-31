-- ==============================================================================
-- Model: dim_weather
-- Layer: Core
-- Materialization: Table
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Dimension table of hourly meteorological conditions for San Francisco.
-- Enables weather-based filtering and slicing across trips.
-- ==============================================================================

{{
    config(
        materialized='table',
        dist='observed_at',
        sort=['observed_at']
    )
}}

select
    {{ generate_surrogate_key(['observed_at']) }}             as weather_key,
    observed_at,
    observed_date,
    observed_hour,
    temperature_c,
    temperature_f,
    precipitation_mm,
    wind_speed_kmh,
    visibility_m,
    weather_code,
    weather_description,
    weather_severity,
    is_raining,
    is_clear,
    is_foggy

from {{ ref('stg_weather') }}
