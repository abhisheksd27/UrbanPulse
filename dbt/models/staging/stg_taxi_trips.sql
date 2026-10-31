-- ==============================================================================
-- Model: stg_taxi_trips
-- Layer: Staging
-- Materialization: View
-- Schema: staging
--
-- WHAT THIS MODEL DOES:
-- Cleans, renames, types, and validates raw SF taxi trip logs from DataSF.
--
-- TRANSFORMATIONS APPLIED:
--   1. Standardizes column names and explicit data types
--   2. Computes derived measures: trip_duration_min, distance_km, total_amount
--   3. Filters out corrupt rows (negative fares, invalid timestamps, out-of-bound coords)
-- ==============================================================================

{{ config(materialized='view') }}

with source_data as (

    select
        coalesce(trip_id, riderid)                                 as trip_id,
        coalesce(vehicle_id, medallion)                            as vehicle_id,
        coalesce(company, taxi_company, 'Unknown')                 as company,
        cast(pickup_datetime as timestamp)                         as pickup_at,
        cast(dropoff_datetime as timestamp)                        as dropoff_at,
        cast(pickup_latitude as decimal(9,6))                      as pickup_lat,
        cast(pickup_longitude as decimal(9,6))                     as pickup_lng,
        cast(dropoff_latitude as decimal(9,6))                     as dropoff_lat,
        cast(dropoff_longitude as decimal(9,6))                    as dropoff_lng,
        cast(coalesce(fare, 0.0) as decimal(10,2))                 as fare_amount,
        cast(coalesce(tips, 0.0) as decimal(10,2))                 as tip_amount,
        cast(coalesce(distance, 0.0) as decimal(10,3))             as distance_miles,
        coalesce(payment_type, 'Unknown')                          as payment_type

    from {{ source('landing', 'taxi_trips') }}

),

transformed as (

    select
        trip_id,
        vehicle_id,
        company,
        pickup_at,
        dropoff_at,
        cast(pickup_at as date)                                    as pickup_date,
        extract(hour from pickup_at)                               as pickup_hour,
        pickup_lat,
        pickup_lng,
        dropoff_lat,
        dropoff_lng,
        fare_amount,
        tip_amount,
        fare_amount + tip_amount                                   as total_amount,
        distance_miles,
        {{ convert_miles_to_km('distance_miles') }}                as distance_km,
        datediff('minute', pickup_at, dropoff_at)                  as trip_duration_min,
        payment_type

    from source_data

    where
        trip_id is not null
        and pickup_at is not null
        and dropoff_at is not null
        and dropoff_at >= pickup_at
        and fare_amount >= 0.0
        and distance_miles > 0.0
        and datediff('minute', pickup_at, dropoff_at) between 1 and 720
        -- Geographic filter: San Francisco & immediate Bay Area envelope
        and pickup_lat between 37.55 and 37.85
        and pickup_lng between -122.55 and -122.30

)

select * from transformed
