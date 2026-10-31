-- ==============================================================================
-- Model: fact_weather_obs
-- Layer: Core
-- Materialization: Table
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Fact table storing hourly weather metrics and conditions in San Francisco.
-- ==============================================================================

{{
    config(
        materialized='table',
        dist='date_key',
        sort=['observed_at']
    )
}}

select
    {{ generate_surrogate_key(['w.observed_at']) }}           as weather_obs_key,
    coalesce(d.date_key, 19000101)                            as date_key,
    w.observed_at,
    w.observed_hour,
    w.temperature_c,
    w.temperature_f,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.visibility_m,
    w.weather_code,
    w.weather_description,
    w.weather_severity,
    w.is_raining,
    w.is_clear,
    w.is_foggy

from {{ ref('stg_weather') }} w
left join {{ ref('dim_date') }} d
    on w.observed_date = d.full_date
