-- ==============================================================================
-- Model: fact_trips
-- Layer: Core
-- Materialization: Incremental (merge on trip_id)
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Central fact table — one row per taxi trip with all measures and FK references.
-- Joins staging trips with:
--   - dim_date       via pickup date
--   - dim_location   via pickup and dropoff coordinates
--   - dim_weather    via nearest hourly weather observation
-- Computes derived metrics: duration, speed, fare_per_km, tip_percentage.
--
-- INCREMENTAL STRATEGY:
-- On each dbt run, only new trips (by pickup_at) are processed.
-- This avoids re-scanning the entire trips table on every run.
--
-- HOW IT CONNECTS:
--   Upstream:   stg_taxi_trips, dim_date, dim_location, dim_weather
--   Downstream: mart_daily_ops, mart_zone_perf, mart_revenue
-- ==============================================================================

-- TODO: Full implementation in Step 5 (dbt Transformation)

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        dist='pickup_at',
        sort=['pickup_at']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['t.trip_id']) }}  as trip_key,
    t.trip_id,

    -- Foreign Keys
    d.date_key                                              as date_key,
    pl.location_key                                         as pickup_location_key,
    dl.location_key                                         as dropoff_location_key,
    w.weather_key                                           as weather_key,

    -- Timestamps
    t.pickup_at,
    t.dropoff_at,

    -- Measures
    datediff('minute', t.pickup_at, t.dropoff_at)          as trip_duration_min,
    t.distance_miles,
    t.distance_miles * 1.60934                             as distance_km,
    t.fare_amount,
    t.tip_amount,
    t.fare_amount + t.tip_amount                           as total_amount,

    -- Derived metrics
    case when t.fare_amount > 0
         then round(t.tip_amount / t.fare_amount * 100, 2)
         else 0 end                                        as tip_percentage,

    case when t.distance_miles > 0
         then round(t.fare_amount / (t.distance_miles * 1.60934), 2)
         else null end                                     as fare_per_km,

    case when datediff('minute', t.pickup_at, t.dropoff_at) > 0
         then round((t.distance_miles * 1.60934) /
                    (datediff('minute', t.pickup_at, t.dropoff_at) / 60.0), 2)
         else null end                                     as speed_kmh,

    t.payment_type,
    t.company

from {{ ref('stg_taxi_trips') }} t

left join {{ ref('dim_date') }} d
    on cast(t.pickup_at as date) = d.full_date

left join {{ ref('dim_location') }} pl
    on round(t.pickup_lat, 4) = pl.latitude
   and round(t.pickup_lng, 4) = pl.longitude

left join {{ ref('dim_location') }} dl
    on round(t.dropoff_lat, 4) = dl.latitude
   and round(t.dropoff_lng, 4) = dl.longitude

left join {{ ref('dim_weather') }} w
    on dateadd('hour', datediff('hour', '2000-01-01', t.pickup_at), '2000-01-01') = w.observed_at

{% if is_incremental() %}
where t.pickup_at > (select max(pickup_at) from {{ this }})
{% endif %}
