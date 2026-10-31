-- ==============================================================================
-- Model: stg_locations
-- Layer: Staging
-- Materialization: View
-- Schema: staging
--
-- WHAT THIS MODEL DOES:
-- Casts and standardizes the static SF neighborhoods reference seed.
-- ==============================================================================

{{ config(materialized='view') }}

select
    cast(neighborhood_id as integer)                           as location_id,
    cast(neighborhood_name as varchar(100))                    as neighborhood_name,
    cast(district as varchar(50))                              as district_name,
    cast(is_downtown as boolean)                               as is_downtown,
    cast(is_airport as boolean)                                as is_airport,
    cast(center_lat as decimal(9,6))                           as center_latitude,
    cast(center_lng as decimal(9,6))                           as center_longitude

from {{ ref('sf_neighborhoods') }}
