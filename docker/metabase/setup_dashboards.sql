-- ==============================================================================
-- UrbanPulse — Metabase & QuickSight Analytics SQL Queries
-- ==============================================================================
-- These queries power the executive and operational dashboards built on top
-- of the dbt dimensional marts (mart_daily_ops, mart_zone_perf, mart_revenue).
-- Compatible with Amazon Redshift and PostgreSQL.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- DASHBOARD 1: DAILY FLEET OPERATIONS & WEATHER IMPACT
-- ------------------------------------------------------------------------------

-- Card 1.1: Daily Fleet Volume vs. Precipitation (Dual-Axis Trend)
-- Visual: Combo Chart (Bars: total_trips, Line: avg_precipitation_mm)
SELECT
    trip_date,
    day_name,
    is_weekend,
    is_rainy_day,
    total_trips,
    active_vehicles,
    avg_trip_distance_km,
    avg_trip_duration_min,
    avg_speed_kmh,
    ROUND(avg_temperature_c, 1) AS avg_temp_c,
    ROUND(avg_precipitation_mm, 2) AS precipitation_mm
FROM marts.mart_daily_ops
ORDER BY trip_date ASC;

-- Card 1.2: Weather Condition Impact on Demand & Unit Economics
-- Visual: Bar Chart (Grouped by weather condition)
SELECT
    dominant_weather,
    is_rainy_day,
    COUNT(DISTINCT trip_date) AS days_observed,
    SUM(total_trips) AS total_trips,
    ROUND(AVG(total_trips), 0) AS avg_trips_per_day,
    ROUND(AVG(avg_speed_kmh), 1) AS avg_fleet_speed_kmh,
    ROUND(AVG(fare_per_km), 2) AS avg_fare_per_km,
    ROUND(AVG(avg_tip_percentage), 1) AS avg_tip_pct,
    ROUND(SUM(total_revenue), 2) AS total_revenue
FROM marts.mart_daily_ops
GROUP BY dominant_weather, is_rainy_day
ORDER BY total_trips DESC;

-- Card 1.3: Fleet Velocity & Congestion Index by Hour
-- Visual: Heatmap / Table
SELECT
    EXTRACT(HOUR FROM f.pickup_at) AS pickup_hour,
    f.company,
    COUNT(f.trip_id) AS trips_completed,
    ROUND(AVG(f.speed_kmh), 1) AS avg_speed_kmh,
    ROUND(AVG(f.duration_minutes), 1) AS avg_duration_minutes,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare
FROM core.fact_trips f
GROUP BY 1, 2
ORDER BY 1, 3 DESC;


-- ------------------------------------------------------------------------------
-- DASHBOARD 2: SAN FRANCISCO ZONE DEMAND & MOBILITY FLOWS
-- ------------------------------------------------------------------------------

-- Card 2.1: Top 15 Origin-Destination Mobility Corridors
-- Visual: Ranked Horizontal Bar Chart / Chord Matrix
SELECT
    pickup_neighborhood,
    dropoff_neighborhood,
    is_inter_district_trip,
    trip_count,
    ROUND(avg_distance_km, 2) AS avg_dist_km,
    ROUND(avg_duration_min, 1) AS avg_dur_min,
    ROUND(avg_speed_kmh, 1) AS avg_speed_kmh,
    ROUND(total_fare_revenue, 2) AS total_fare,
    ROUND(fare_per_km, 2) AS fare_per_km
FROM marts.mart_zone_perf
ORDER BY trip_count DESC
LIMIT 15;

-- Card 2.2: SFO Airport Connection Dynamics
-- Visual: KPI Scorecard & Summary
SELECT
    CASE 
        WHEN pickup_neighborhood = 'SFO Airport' THEN 'Outbound from SFO'
        WHEN dropoff_neighborhood = 'SFO Airport' THEN 'Inbound to SFO'
        ELSE 'Non-Airport Trip'
    END AS airport_flow_type,
    SUM(trip_count) AS total_trips,
    ROUND(SUM(total_fare_revenue), 2) AS total_revenue,
    ROUND(AVG(avg_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(avg_duration_min), 1) AS avg_duration_min,
    ROUND(AVG(fare_per_km), 2) AS avg_fare_per_km
FROM marts.mart_zone_perf
GROUP BY 1
ORDER BY 2 DESC;

-- Card 2.3: Neighborhood Inflow vs Outflow Net Mobility Balance
-- Visual: Diverging Bar Chart
WITH outflow AS (
    SELECT pickup_neighborhood AS neighborhood, SUM(trip_count) AS departures
    FROM marts.mart_zone_perf
    GROUP BY 1
),
inflow AS (
    SELECT dropoff_neighborhood AS neighborhood, SUM(trip_count) AS arrivals
    FROM marts.mart_zone_perf
    GROUP BY 1
)
SELECT
    COALESCE(o.neighborhood, i.neighborhood) AS neighborhood,
    COALESCE(o.departures, 0) AS departures,
    COALESCE(i.arrivals, 0) AS arrivals,
    COALESCE(i.arrivals, 0) - COALESCE(o.departures, 0) AS net_mobility_balance
FROM outflow o
FULL OUTER JOIN inflow i ON o.neighborhood = i.neighborhood
ORDER BY departures DESC;


-- ------------------------------------------------------------------------------
-- DASHBOARD 3: REVENUE, TIPS & UNIT ECONOMICS
-- ------------------------------------------------------------------------------

-- Card 3.1: Market Share & Revenue by Taxi Operator
-- Visual: Donut Chart / Table
SELECT
    company,
    SUM(total_trips) AS total_trips,
    ROUND(SUM(gross_revenue), 2) AS total_gross_revenue,
    ROUND(SUM(fare_revenue), 2) AS total_fare_revenue,
    ROUND(SUM(tip_revenue), 2) AS total_tip_revenue,
    ROUND(AVG(avg_fare_per_trip), 2) AS avg_fare_per_trip,
    ROUND(AVG(tip_rate_pct), 1) AS overall_tip_pct,
    ROUND(AVG(effective_fare_per_km), 2) AS avg_fare_per_km
FROM marts.mart_revenue
GROUP BY company
ORDER BY total_gross_revenue DESC;

-- Card 3.2: Payment Method Breakdown & Digital Tipping Propensity
-- Visual: 100% Stacked Bar Chart
SELECT
    r.trip_date,
    r.payment_type,
    SUM(r.total_trips) AS trips_count,
    ROUND(SUM(r.gross_revenue), 2) AS revenue,
    ROUND(SUM(r.tip_revenue), 2) AS tips,
    ROUND(AVG(r.tip_rate_pct), 1) AS tip_pct
FROM marts.mart_revenue r
GROUP BY r.trip_date, r.payment_type
ORDER BY r.trip_date, revenue DESC;

-- Card 3.3: High-Value Travel Windows (Revenue per Hour)
-- Visual: Multi-Line Chart
SELECT
    EXTRACT(DOW FROM pickup_at) AS day_of_week_num,
    EXTRACT(HOUR FROM pickup_at) AS hour_of_day,
    COUNT(trip_id) AS trips_completed,
    ROUND(SUM(total_amount), 2) AS total_hourly_revenue,
    ROUND(AVG(fare_per_km), 2) AS effective_fare_per_km
FROM core.fact_trips
GROUP BY 1, 2
ORDER BY 1, 2;
