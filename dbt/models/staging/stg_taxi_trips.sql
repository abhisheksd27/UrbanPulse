-- ==============================================================================
-- Model: stg_taxi_trips
-- Layer: Staging
-- Materialization: View
-- Schema: staging
--
-- WHAT THIS MODEL DOES:
-- Renames, casts, and lightly validates raw SF taxi trip records
-- from the DataSF API that were loaded via Redshift COPY into landing.taxi_trips.
--
-- TRANSFORMATIONS APPLIED:
--   1. Rename columns from DataSF snake_case to our internal conventions
--   2. Cast all string columns to their correct data types
--   3. Convert pickup/dropoff timestamps to UTC
--   4. Filter out obviously invalid records (negative fares, zero distances)
--
-- HOW IT CONNECTS:
--   Upstream:   landing.taxi_trips  (raw Redshift COPY from S3)
--   Downstream: core.fact_trips     (uses this as the primary trip source)
--               core.dim_location   (uses pickup/dropoff coordinates)
-- ==============================================================================

-- TODO: Implement in Step 5 (dbt Transformation)
-- This is a placeholder — the full SQL is written in Step 5.

select
    riderid                                        as trip_id,
    vehicle_id,
    company,
    cast(pickup_datetime as timestamp)             as pickup_at,
    cast(dropoff_datetime as timestamp)            as dropoff_at,
    cast(pickup_latitude as decimal(9,6))          as pickup_lat,
    cast(pickup_longitude as decimal(9,6))         as pickup_lng,
    cast(dropoff_latitude as decimal(9,6))         as dropoff_lat,
    cast(dropoff_longitude as decimal(9,6))        as dropoff_lng,
    cast(fare as decimal(10,2))                    as fare_amount,
    cast(tips as decimal(10,2))                    as tip_amount,
    cast(distance as decimal(10,3))                as distance_miles,
    payment_type

from {{ source('landing', 'taxi_trips') }}

where
    -- Remove obviously invalid trips
    cast(fare as decimal(10,2)) >= 0
    and cast(distance as decimal(10,3)) > 0
    and pickup_datetime is not null
    and dropoff_datetime is not null
