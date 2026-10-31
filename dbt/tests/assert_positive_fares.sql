-- ==============================================================================
-- Custom Data Test: assert_positive_fares
-- Passes if 0 rows are returned (no negative fare amounts in fact_trips).
-- ==============================================================================

select
    trip_key,
    trip_id,
    fare_amount
from {{ ref('fact_trips') }}
where fare_amount < 0.0
