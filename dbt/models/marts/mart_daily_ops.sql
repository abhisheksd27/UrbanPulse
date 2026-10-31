-- ==============================================================================
-- Model: mart_daily_ops
-- Layer: Marts
-- Materialization: Table
-- Schema: marts
--
-- WHAT THIS MODEL DOES:
-- Daily operational summary: one row per calendar day.
-- Powers the Metabase / QuickSight "Daily Fleet Operations" dashboard.
-- ==============================================================================

{{
    config(
        materialized='table',
        sort=['report_date']
    )
}}

with trips as (

    select
        ft.trip_key,
        ft.trip_id,
        ft.date_key,
        ft.pickup_location_key,
        ft.pickup_at,
        extract(hour from ft.pickup_at)                         as pickup_hour,
        ft.trip_duration_min,
        ft.distance_km,
        ft.speed_kmh,
        ft.fare_amount,
        ft.tip_amount,
        ft.total_amount,
        ft.tip_percentage,
        w.is_raining,
        loc.is_airport

    from {{ ref('fact_trips') }} ft
    left join {{ ref('dim_weather') }} w
        on ft.weather_key = w.weather_key
    left join {{ ref('dim_location') }} loc
        on ft.pickup_location_key = loc.location_key

),

daily_base as (

    select
        d.full_date                                            as report_date,
        d.year,
        d.month,
        d.day_name,
        d.is_weekend,
        d.is_holiday,

        -- Volume Metrics
        count(t.trip_key)                                      as total_trips,
        count(case when t.is_airport then 1 end)               as airport_trips,
        count(case when t.is_raining then 1 end)               as rainy_trips,

        -- Revenue Metrics
        coalesce(round(sum(t.total_amount), 2), 0.0)           as total_revenue,
        coalesce(round(avg(t.fare_amount), 2), 0.0)            as avg_fare,
        coalesce(round(avg(t.tip_amount), 2), 0.0)             as avg_tip,
        coalesce(round(avg(t.tip_percentage), 2), 0.0)         as avg_tip_percentage,

        -- Fleet Trip Performance
        coalesce(round(avg(t.trip_duration_min), 2), 0.0)      as avg_duration_min,
        coalesce(round(avg(t.distance_km), 2), 0.0)            as avg_distance_km,
        coalesce(round(avg(t.speed_kmh), 2), 0.0)              as avg_speed_kmh

    from {{ ref('dim_date') }} d
    inner join trips t
        on d.date_key = t.date_key

    group by
        d.full_date,
        d.year,
        d.month,
        d.day_name,
        d.is_weekend,
        d.is_holiday

),

-- Calculate peak demand hour per day
peak_hours as (

    select
        date_key,
        pickup_hour as peak_hour
    from (
        select
            date_key,
            pickup_hour,
            count(*) as trips_in_hour,
            row_number() over (partition by date_key order by count(*) desc) as rnk
        from trips
        group by date_key, pickup_hour
    ) ranked
    where rnk = 1

)

select
    b.report_date,
    b.year,
    b.month,
    b.day_name,
    b.is_weekend,
    b.is_holiday,
    b.total_trips,
    b.airport_trips,
    b.rainy_trips,
    b.total_revenue,
    b.avg_fare,
    b.avg_tip,
    b.avg_tip_percentage,
    b.avg_duration_min,
    b.avg_distance_km,
    b.avg_speed_kmh,
    coalesce(p.peak_hour, 12)                                  as peak_hour

from daily_base b
left join {{ ref('dim_date') }} d
    on b.report_date = d.full_date
left join peak_hours p
    on d.date_key = p.date_key

order by b.report_date desc
