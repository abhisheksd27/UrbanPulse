-- ==============================================================================
-- Custom Data Test: assert_valid_total_amount
-- Passes if 0 rows are returned (total_amount must equal fare_amount + tip_amount).
-- ==============================================================================

select
    trip_key,
    trip_id,
    fare_amount,
    tip_amount,
    total_amount,
    abs(total_amount - (fare_amount + tip_amount)) as reconciliation_delta
from {{ ref('fact_trips') }}
where abs(total_amount - (fare_amount + tip_amount)) > 0.02
