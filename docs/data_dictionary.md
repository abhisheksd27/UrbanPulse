# Data Dictionary — UrbanPulse

> This document defines every field in every table across the platform.
> It is the authoritative source of truth for column names, types, and business meaning.
> Keep this in sync with dbt schema.yml files.

---

## Table of Contents

1. [Source: SF Taxi Trips (Raw)](#1-source-sf-taxi-trips-raw)
2. [Source: Weather (Raw)](#2-source-weather-raw)
3. [Staging: stg_taxi_trips](#3-staging-stg_taxi_trips)
4. [Core: dim_date](#4-core-dim_date)
5. [Core: dim_location](#5-core-dim_location)
6. [Core: dim_weather](#6-core-dim_weather)
7. [Core: fact_trips](#7-core-fact_trips)
8. [Mart: mart_daily_ops](#8-mart-mart_daily_ops)
9. [Mart: mart_zone_perf](#9-mart-mart_zone_perf)
10. [Mart: mart_revenue](#10-mart-mart_revenue)

---

## 1. Source: SF Taxi Trips (Raw)

**S3 Path**: `s3://urbanpulse-lake-{env}/raw/taxi/year=YYYY/month=MM/trips.parquet`
**Source**: DataSF API (`data.sfgov.org/resource/yitu-d5am.json`)
**Updated**: Monthly

| Column (Raw) | Type | Description |
|---|---|---|
| `riderid` | string | Unique trip identifier assigned by SFMTA |
| `vehicle_id` | string | Anonymized taxi vehicle identifier |
| `company` | string | Taxi company name |
| `pickup_datetime` | string | Trip pickup timestamp (ISO 8601 string) |
| `dropoff_datetime` | string | Trip dropoff timestamp (ISO 8601 string) |
| `pickup_longitude` | string | Pickup location longitude |
| `pickup_latitude` | string | Pickup location latitude |
| `dropoff_longitude` | string | Dropoff location longitude |
| `dropoff_latitude` | string | Dropoff location latitude |
| `fare` | string | Base fare amount (USD) |
| `tips` | string | Tip amount (USD) |
| `distance` | string | Trip distance in miles |
| `payment_type` | string | Payment method (Cash, Credit Card, etc.) |

---

## 2. Source: Weather (Raw)

**S3 Path**: `s3://urbanpulse-lake-{env}/raw/weather/date=YYYY-MM-DD/weather.parquet`
**Source**: Open-Meteo Historical API
**Updated**: Daily

| Column (Raw) | Type | Description |
|---|---|---|
| `time` | string | Observation timestamp (hourly, ISO 8601) |
| `temperature_2m` | float | Air temperature at 2 metres height (°C) |
| `precipitation` | float | Total precipitation in the hour (mm) |
| `windspeed_10m` | float | Wind speed at 10 metres (km/h) |
| `visibility` | float | Visibility (metres) |
| `weathercode` | integer | WMO weather code (0=clear sky, 61=rain, 71=snow, etc.) |

---

## 3. Staging: stg_taxi_trips

**Redshift Location**: `core.stg_taxi_trips`
**dbt Model**: `dbt/models/staging/stg_taxi_trips.sql`
**Description**: Cleaned, renamed, and type-cast version of the raw SF taxi data.

| Column | Type | Description | Source Column |
|---|---|---|---|
| `trip_id` | varchar(50) | Unique trip identifier | `riderid` |
| `vehicle_id` | varchar(50) | Anonymized taxi vehicle identifier | `vehicle_id` |
| `company` | varchar(100) | Taxi company name | `company` |
| `pickup_at` | timestamp | Trip pickup datetime (UTC) | `pickup_datetime` |
| `dropoff_at` | timestamp | Trip dropoff datetime (UTC) | `dropoff_datetime` |
| `pickup_lat` | decimal(9,6) | Pickup latitude | `pickup_latitude` |
| `pickup_lng` | decimal(9,6) | Pickup longitude | `pickup_longitude` |
| `dropoff_lat` | decimal(9,6) | Dropoff latitude | `dropoff_latitude` |
| `dropoff_lng` | decimal(9,6) | Dropoff longitude | `dropoff_longitude` |
| `fare_amount` | decimal(10,2) | Base fare in USD | `fare` |
| `tip_amount` | decimal(10,2) | Tip amount in USD | `tips` |
| `distance_miles` | decimal(10,3) | Trip distance in miles | `distance` |
| `payment_type` | varchar(30) | Payment method | `payment_type` |

---

## 4. Core: dim_date

**Redshift Location**: `core.dim_date`
**dbt Model**: `dbt/models/core/dim_date.sql`
**Description**: Date dimension generated for the range 2022-01-01 to 2025-12-31.

| Column | Type | Description |
|---|---|---|
| `date_key` | integer | Surrogate key (YYYYMMDD format, e.g. 20240115) |
| `full_date` | date | The calendar date |
| `year` | integer | 4-digit year (e.g. 2024) |
| `quarter` | integer | Quarter number (1-4) |
| `month` | integer | Month number (1-12) |
| `month_name` | varchar(10) | Month name (e.g. 'January') |
| `week_of_year` | integer | ISO week number (1-53) |
| `day_of_month` | integer | Day of month (1-31) |
| `day_of_week` | integer | Day of week (1=Monday, 7=Sunday) |
| `day_name` | varchar(10) | Day name (e.g. 'Monday') |
| `is_weekend` | boolean | True if Saturday or Sunday |
| `is_holiday` | boolean | True if US federal holiday |
| `holiday_name` | varchar(50) | Holiday name if applicable (else NULL) |

---

## 5. Core: dim_location

**Redshift Location**: `core.dim_location`
**dbt Model**: `dbt/models/core/dim_location.sql`
**Description**: SF location dimension built from unique lat/lng coordinates, enriched with neighborhood data.

| Column | Type | Description |
|---|---|---|
| `location_key` | integer | Surrogate key (auto-increment) |
| `latitude` | decimal(9,6) | Location latitude (rounded to 4 decimal places) |
| `longitude` | decimal(9,6) | Location longitude (rounded to 4 decimal places) |
| `neighborhood` | varchar(100) | SF neighborhood name from GeoJSON lookup |
| `district` | varchar(50) | SF supervisor district |
| `is_airport` | boolean | True if location is within SFO airport bounds |
| `is_downtown` | boolean | True if location is in downtown SF zones |

---

## 6. Core: dim_weather

**Redshift Location**: `core.dim_weather`
**dbt Model**: `dbt/models/core/dim_weather.sql`
**Description**: Hourly weather observations for San Francisco.

| Column | Type | Description |
|---|---|---|
| `weather_key` | integer | Surrogate key (auto-increment) |
| `observed_at` | timestamp | Observation timestamp (hourly, UTC) |
| `temperature_c` | decimal(5,2) | Temperature in Celsius |
| `precipitation_mm` | decimal(6,2) | Precipitation in millimetres |
| `wind_speed_kmh` | decimal(6,2) | Wind speed in km/h |
| `visibility_m` | integer | Visibility in metres |
| `weather_code` | integer | WMO weather code |
| `weather_description` | varchar(50) | Human-readable weather (Clear, Rain, Snow, etc.) |
| `is_raining` | boolean | True if precipitation > 0.1mm |
| `is_clear` | boolean | True if weathercode = 0 |

---

## 7. Core: fact_trips

**Redshift Location**: `core.fact_trips`
**dbt Model**: `dbt/models/core/fact_trips.sql`
**Description**: Central fact table — one row per taxi trip. All numeric measures and FK references.

| Column | Type | Description |
|---|---|---|
| `trip_key` | bigint | Surrogate key |
| `trip_id` | varchar(50) | Natural key from source |
| `date_key` | integer | FK → dim_date |
| `pickup_location_key` | integer | FK → dim_location (pickup) |
| `dropoff_location_key` | integer | FK → dim_location (dropoff) |
| `weather_key` | integer | FK → dim_weather (nearest hourly obs) |
| `pickup_at` | timestamp | Pickup datetime |
| `dropoff_at` | timestamp | Dropoff datetime |
| `trip_duration_min` | decimal(8,2) | Trip duration in minutes |
| `distance_miles` | decimal(10,3) | Trip distance in miles |
| `distance_km` | decimal(10,3) | Trip distance in kilometres |
| `fare_amount` | decimal(10,2) | Base fare (USD) |
| `tip_amount` | decimal(10,2) | Tip (USD) |
| `total_amount` | decimal(10,2) | Total = fare + tip (USD) |
| `tip_percentage` | decimal(5,2) | Tip as % of fare |
| `fare_per_km` | decimal(8,2) | Fare efficiency (USD/km) |
| `speed_kmh` | decimal(8,2) | Average trip speed (km/h) |
| `payment_type` | varchar(30) | Payment method |
| `company` | varchar(100) | Taxi company |

---

## 8. Mart: mart_daily_ops

**Redshift Location**: `marts.mart_daily_ops`
**dbt Model**: `dbt/models/marts/mart_daily_ops.sql`
**Description**: Daily operational summary — one row per day. Powers the main ops dashboard.

| Column | Type | Description |
|---|---|---|
| `report_date` | date | Calendar date |
| `total_trips` | integer | Total number of completed trips |
| `total_revenue` | decimal(12,2) | Sum of total_amount (USD) |
| `avg_fare` | decimal(10,2) | Average fare per trip (USD) |
| `avg_duration_min` | decimal(8,2) | Average trip duration (minutes) |
| `avg_distance_km` | decimal(8,2) | Average trip distance (km) |
| `avg_speed_kmh` | decimal(8,2) | Average speed across all trips |
| `peak_hour` | integer | Hour of day with most trips (0-23) |
| `top_pickup_neighborhood` | varchar(100) | Neighborhood with most pickups |
| `avg_tip_percentage` | decimal(5,2) | Average tip % across all trips |
| `rainy_day` | boolean | True if any rain recorded that day |

---

## 9. Mart: mart_zone_perf

**Redshift Location**: `marts.mart_zone_perf`
**dbt Model**: `dbt/models/marts/mart_zone_perf.sql`
**Description**: Neighborhood-level performance — one row per neighborhood per month.

| Column | Type | Description |
|---|---|---|
| `report_month` | date | First day of the month |
| `neighborhood` | varchar(100) | SF neighborhood name |
| `total_pickups` | integer | Total trips starting in this zone |
| `total_dropoffs` | integer | Total trips ending in this zone |
| `avg_fare` | decimal(10,2) | Average fare for trips starting here |
| `avg_duration_min` | decimal(8,2) | Average trip duration from this zone |
| `avg_wait_time_min` | decimal(8,2) | TODO: from streaming data |
| `revenue_rank` | integer | Zone rank by total revenue (1 = highest) |

---

## 10. Mart: mart_revenue

**Redshift Location**: `marts.mart_revenue`
**dbt Model**: `dbt/models/marts/mart_revenue.sql`
**Description**: Revenue breakdown by month and payment type. Powers the revenue dashboard.

| Column | Type | Description |
|---|---|---|
| `report_month` | date | First day of the month |
| `payment_type` | varchar(30) | Payment method |
| `company` | varchar(100) | Taxi company |
| `total_trips` | integer | Number of trips |
| `total_revenue` | decimal(12,2) | Total revenue (USD) |
| `avg_fare` | decimal(10,2) | Average fare per trip |
| `avg_tip` | decimal(10,2) | Average tip per trip |
| `avg_tip_percentage` | decimal(5,2) | Average tip as % of fare |
| `revenue_share_pct` | decimal(5,2) | This row's % of total month revenue |
