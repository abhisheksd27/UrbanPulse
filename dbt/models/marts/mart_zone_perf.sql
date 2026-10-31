-- ==============================================================================
-- Model: mart_zone_perf
-- Layer: Marts
-- Materialization: Table
-- Schema: marts
--
-- WHAT THIS MODEL DOES:
-- Neighborhood zone performance: one row per neighborhood per calendar month.
-- Evaluates origin/destination demand patterns and ranks zones by revenue.
-- ==============================================================================

{{
    config(
        materialized='table',
        sort=['report_month', 'revenue_rank']
    )
}}

with pickup_stats as (

    select
        date_trunc('month', ft.pickup_at)                      as report_month,
        ft.pickup_location_key                                 as location_key,
        count(ft.trip_key)                                     as total_pickups,
        sum(ft.total_amount)                                   as total_pickup_revenue,
        avg(ft.fare_amount)                                    as avg_pickup_fare,
        avg(ft.trip_duration_min)                              as avg_pickup_duration_min,
        avg(ft.distance_km)                                    as avg_pickup_distance_km

    from {{ ref('fact_trips') }} ft
    group by 1, 2

),

dropoff_stats as (

    select
        date_trunc('month', ft.pickup_at)                      as report_month,
        ft.dropoff_location_key                                as location_key,
        count(ft.trip_key)                                     as total_dropoffs

    from {{ ref('fact_trips') }} ft
    group by 1, 2

),

combined as (

    select
        p.report_month,
        loc.neighborhood_name,
        loc.district_name,
        loc.is_downtown,
        loc.is_airport,
        p.total_pickups,
        coalesce(d.total_dropoffs, 0)                          as total_dropoffs,
        round(p.total_pickup_revenue, 2)                       as total_pickup_revenue,
        round(p.avg_pickup_fare, 2)                            as avg_pickup_fare,
        round(p.avg_pickup_duration_min, 2)                    as avg_trip_duration_min,
        round(p.avg_pickup_distance_km, 2)                     as avg_trip_distance_km,

        dense_rank() over (
            partition by p.report_month
            order by p.total_pickup_revenue desc
        )                                                      as revenue_rank

    from pickup_stats p
    inner join {{ ref('dim_location') }} loc
        on p.location_key = loc.location_key
    left join dropoff_stats d
        on p.report_month = d.report_month
       and p.location_key = d.location_key

)

select * from combined
order by report_month desc, revenue_rank asc
