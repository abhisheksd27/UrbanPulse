-- ==============================================================================
-- Custom Data Test: assert_valid_trip_duration
-- Passes if 0 rows are returned (all trips must have duration between 1 min and 12 hrs).
-- ==============================================================================

select
    trip_key,
    trip_id,
    trip_duration_min
from {{ ref('fact_trips') }}
where trip_duration_min <= 0 or trip_duration_min > 720
