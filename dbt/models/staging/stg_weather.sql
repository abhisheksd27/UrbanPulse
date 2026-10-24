-- ==============================================================================
-- Model: stg_weather
-- Layer: Staging
-- Materialization: View
-- Schema: staging
--
-- WHAT THIS MODEL DOES:
-- Casts and renames Open-Meteo hourly weather observations.
-- Adds a human-readable weather_description from the WMO code.
-- Adds convenience boolean flags (is_raining, is_clear).
--
-- HOW IT CONNECTS:
--   Upstream:   landing.weather_observations (raw COPY from S3)
--   Downstream: core.dim_weather             (builds the weather dimension)
--               core.fact_trips              (joined by nearest hour)
-- ==============================================================================

-- TODO: Implement in Step 5 (dbt Transformation)

select
    cast(time as timestamp)                                as observed_at,
    cast(temperature_2m as decimal(5,2))                   as temperature_c,
    cast(precipitation as decimal(6,2))                    as precipitation_mm,
    cast(windspeed_10m as decimal(6,2))                    as wind_speed_kmh,
    cast(visibility as integer)                            as visibility_m,
    cast(weathercode as integer)                           as weather_code,

    -- Convenience flags
    case when cast(precipitation as decimal(6,2)) > 0.1
         then true else false end                          as is_raining,
    case when cast(weathercode as integer) = 0
         then true else false end                         as is_clear

from {{ source('landing', 'weather_observations') }}

where time is not null
