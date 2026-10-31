-- ==============================================================================
-- Model: dim_location
-- Layer: Core
-- Materialization: Table (diststyle all)
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Dimension table for San Francisco analysis neighborhoods and districts.
-- Replicated to all Redshift compute slices (diststyle all) for fast broadcast joins.
-- ==============================================================================

{{
    config(
        materialized='table',
        dist='all',
        sort=['location_id']
    )
}}

select
    {{ generate_surrogate_key(['location_id']) }}             as location_key,
    location_id,
    neighborhood_name,
    district_name,
    is_downtown,
    is_airport,
    center_latitude,
    center_longitude

from {{ ref('stg_locations') }}
