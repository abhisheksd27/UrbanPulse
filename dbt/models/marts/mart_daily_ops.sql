-- ==============================================================================
-- Model: mart_daily_ops
-- Layer: Marts
-- Materialization: Table
-- Schema: marts
--
-- WHAT THIS MODEL DOES:
-- Pre-aggregates trip data to the daily level for operational dashboards.
-- One row per calendar day.
-- Powers the "Daily Operations" dashboard in Metabase/QuickSight.
--
-- KEY METRICS:
--   - total_trips: How busy was today?
--   - total_revenue: How much did the fleet earn?
--   - avg_fare/duration/distance: What does a typical trip look like?
--   - peak_hour: When is demand highest?
--   - top_pickup_neighborhood: Where are most trips starting?
--
-- HOW IT CONNECTS:
--   Upstream:   fact_trips, dim_date, dim_location
--   Downstream: Metabase "Daily Ops" dashboard
-- ==============================================================================

-- TODO: Full implementation in Step 5 (dbt Transformation)

select
    d.full_date                                            as report_date,
    d.day_name,
    d.is_weekend,
    d.is_holiday,

    -- Volume metrics
    count(ft.trip_key)                                     as total_trips,

    -- Revenue metrics
    round(sum(ft.total_amount), 2)                         as total_revenue,
    round(avg(ft.fare_amount), 2)                          as avg_fare,
    round(avg(ft.tip_percentage), 2)                       as avg_tip_percentage,

    -- Trip characteristics
    round(avg(ft.trip_duration_min), 2)                    as avg_duration_min,
    round(avg(ft.distance_km), 2)                          as avg_distance_km,
    round(avg(ft.speed_kmh), 2)                            as avg_speed_kmh,

    -- Peak demand
    mode() within group (order by extract(hour from ft.pickup_at))
                                                           as peak_hour,

    -- Top zone (subquery — will be extracted to macro in Step 5)
    (
        select l.neighborhood
        from {{ ref('fact_trips') }} ft2
        join {{ ref('dim_location') }} l on ft2.pickup_location_key = l.location_key
        where cast(ft2.pickup_at as date) = d.full_date
        group by l.neighborhood
        order by count(*) desc
        limit 1
    )                                                      as top_pickup_neighborhood

from {{ ref('dim_date') }} d
left join {{ ref('fact_trips') }} ft
    on d.date_key = ft.date_key

where d.full_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'

group by 1, 2, 3, 4
order by report_date desc
