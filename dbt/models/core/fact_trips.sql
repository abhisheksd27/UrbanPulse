-- ==============================================================================
-- Model: fact_trips
-- Layer: Core
-- Materialization: Incremental (merge on trip_id)
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Central Star Schema fact table: one row per completed taxi trip.
-- Joins with:
--   - dim_date (by calendar date)
--   - dim_location (by nearest neighborhood centroid for pickup & dropoff)
--   - dim_weather (by nearest hourly observation)
--
-- INCREMENTAL STRATEGY:
-- Scans and appends/merges only records with pickup_at greater than current max.
-- ==============================================================================

{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        dist='date_key',
        sort=['pickup_at']
    )
}}

with trips as (

    select *
    from {{ ref('stg_taxi_trips') }}

    {% if is_incremental() %}
    where pickup_at > (select coalesce(max(pickup_at), '2000-01-01') from {{ this }})
    {% endif %}

),

locations as (

    select
        location_key,
        location_id,
        neighborhood_name,
        center_latitude,
        center_longitude
    from {{ ref('dim_location') }}

),

-- Resolve nearest SF neighborhood centroid for pickup location
pickup_locations as (

    select
        trip_id,
        location_key as pickup_location_key
    from (
        select
            t.trip_id,
            l.location_key,
            row_number() over (
                partition by t.trip_id
                order by (
                    (t.pickup_lat - l.center_latitude) * (t.pickup_lat - l.center_latitude) +
                    (t.pickup_lng - l.center_longitude) * (t.pickup_lng - l.center_longitude) * 0.62
                ) asc
            ) as rnk
        from trips t
        cross join locations l
    ) ranked
    where rnk = 1

),

-- Resolve nearest SF neighborhood centroid for dropoff location
dropoff_locations as (

    select
        trip_id,
        location_key as dropoff_location_key
    from (
        select
            t.trip_id,
            l.location_key,
            row_number() over (
                partition by t.trip_id
                order by (
                    (t.dropoff_lat - l.center_latitude) * (t.dropoff_lat - l.center_latitude) +
                    (t.dropoff_lng - l.center_longitude) * (t.dropoff_lng - l.center_longitude) * 0.62
                ) asc
            ) as rnk
        from trips t
        cross join locations l
    ) ranked
    where rnk = 1

),

final_fact as (

    select
        {{ generate_surrogate_key(['t.trip_id']) }}            as trip_key,
        t.trip_id,
        t.vehicle_id,
        t.company,

        -- Foreign Keys
        coalesce(d.date_key, 19000101)                         as date_key,
        pl.pickup_location_key,
        dl.dropoff_location_key,
        w.weather_key,

        -- Timestamps
        t.pickup_at,
        t.dropoff_at,

        -- Measures
        t.trip_duration_min,
        t.distance_miles,
        t.distance_km,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,

        -- Advanced KPIs
        {{ safe_divide('t.distance_km', '(t.trip_duration_min / 60.0)', 2, 'null') }} as speed_kmh,
        {{ safe_divide('t.fare_amount', 't.distance_km', 2, 'null') }}                as fare_per_km,
        {{ safe_divide('(t.tip_amount * 100.0)', 't.fare_amount', 2, '0.0') }}       as tip_percentage,

        -- Operational attributes
        t.payment_type

    from trips t

    left join {{ ref('dim_date') }} d
        on t.pickup_date = d.full_date

    left join pickup_locations pl
        on t.trip_id = pl.trip_id

    left join dropoff_locations dl
        on t.trip_id = dl.trip_id

    left join {{ ref('dim_weather') }} w
        on date_trunc('hour', t.pickup_at) = w.observed_at

)

select * from final_fact
