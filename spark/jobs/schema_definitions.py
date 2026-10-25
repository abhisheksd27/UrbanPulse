"""
UrbanPulse - Centralized PySpark Schemas & Contracts
====================================================
Defines canonical schemas for all data lake layers:
  - Bronze (Raw): Ingested source representations
  - Silver (Staging): Cleaned, typed, deduplicated, and spatially enriched
  - Gold (Curated): Analytical facts with weather joins and pre-aggregated marts

Can be used directly in PySpark (`StructType`) or imported as dictionary
specifications for schema validation across the platform.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Check if PySpark is available in the current environment
try:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


# ==============================================================================
# 1. BRONZE (RAW) SCHEMAS
# ==============================================================================

# Dictionary schema contract for Raw SF Taxi
RAW_TAXI_COLUMNS: Dict[str, str] = {
    "trip_id": "string",
    "vehicle_id": "string",
    "company": "string",
    "pickup_datetime": "string",
    "dropoff_datetime": "string",
    "pickup_latitude": "double",
    "pickup_longitude": "double",
    "dropoff_latitude": "double",
    "dropoff_longitude": "double",
    "fare": "double",
    "tips": "double",
    "distance": "double",
    "payment_type": "string",
    "ingested_at": "string",
}

# Dictionary schema contract for Raw Open-Meteo Weather
RAW_WEATHER_COLUMNS: Dict[str, str] = {
    "time": "string",
    "temperature_2m": "double",
    "precipitation": "double",
    "windspeed_10m": "double",
    "visibility": "double",
    "weathercode": "integer",
    "ingested_at": "string",
}

# Dictionary schema contract for Raw SF Neighborhoods Geospatial
RAW_GEO_COLUMNS: Dict[str, str] = {
    "id": "integer",
    "neighborhood": "string",
    "district": "string",
    "center_lat": "double",
    "center_lng": "double",
    "is_downtown": "boolean",
    "is_airport": "boolean",
}


# ==============================================================================
# 2. SILVER (STAGING) SCHEMAS
# ==============================================================================

SILVER_TAXI_COLUMNS: Dict[str, str] = {
    "trip_id": "string",
    "vehicle_id": "string",
    "company": "string",
    "pickup_at": "timestamp",
    "dropoff_at": "timestamp",
    "pickup_date": "date",
    "pickup_hour": "integer",
    "pickup_lat": "double",
    "pickup_lng": "double",
    "dropoff_lat": "double",
    "dropoff_lng": "double",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
    "distance_miles": "double",
    "distance_km": "double",
    "duration_minutes": "double",
    "payment_type": "string",
    "pickup_neighborhood": "string",
    "pickup_district": "string",
    "dropoff_neighborhood": "string",
    "dropoff_district": "string",
    "is_airport_trip": "boolean",
    "is_downtown_trip": "boolean",
    "processed_at": "timestamp",
}

SILVER_WEATHER_COLUMNS: Dict[str, str] = {
    "observed_at": "timestamp",
    "temperature_c": "double",
    "precipitation_mm": "double",
    "wind_speed_kmh": "double",
    "visibility_m": "double",
    "weather_code": "integer",
    "weather_description": "string",
    "is_raining": "boolean",
    "is_clear": "boolean",
    "processed_at": "timestamp",
}


# ==============================================================================
# 3. GOLD (CURATED) SCHEMAS
# ==============================================================================

GOLD_FACT_TRIPS_COLUMNS: Dict[str, str] = {
    "trip_id": "string",
    "vehicle_id": "string",
    "company": "string",
    "pickup_at": "timestamp",
    "dropoff_at": "timestamp",
    "pickup_date": "date",
    "pickup_hour": "integer",
    "pickup_neighborhood": "string",
    "pickup_district": "string",
    "dropoff_neighborhood": "string",
    "dropoff_district": "string",
    "is_airport_trip": "boolean",
    "is_downtown_trip": "boolean",
    "distance_miles": "double",
    "distance_km": "double",
    "duration_minutes": "double",
    "speed_kmh": "double",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
    "tip_percentage": "double",
    "fare_per_km": "double",
    "payment_type": "string",
    # Joined weather context
    "temperature_c": "double",
    "precipitation_mm": "double",
    "weather_condition": "string",
    "is_raining": "boolean",
    "curated_at": "timestamp",
}


# ==============================================================================
# 4. PYSPARK STRUCTTYPE DEFINITIONS (when pyspark is installed)
# ==============================================================================

if PYSPARK_AVAILABLE:
    RAW_TAXI_SPARK_SCHEMA = StructType([
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), True),
        StructField("company", StringType(), True),
        StructField("pickup_datetime", StringType(), False),
        StructField("dropoff_datetime", StringType(), False),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("fare", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("distance", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ])

    RAW_WEATHER_SPARK_SCHEMA = StructType([
        StructField("time", StringType(), False),
        StructField("temperature_2m", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windspeed_10m", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("weathercode", IntegerType(), True),
        StructField("ingested_at", StringType(), True),
    ])

    SILVER_TAXI_SPARK_SCHEMA = StructType([
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), True),
        StructField("company", StringType(), True),
        StructField("pickup_at", TimestampType(), False),
        StructField("dropoff_at", TimestampType(), False),
        StructField("pickup_date", StringType(), False),
        StructField("pickup_hour", IntegerType(), False),
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lng", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lng", DoubleType(), True),
        StructField("fare_amount", DoubleType(), False),
        StructField("tip_amount", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("distance_miles", DoubleType(), False),
        StructField("distance_km", DoubleType(), False),
        StructField("duration_minutes", DoubleType(), False),
        StructField("payment_type", StringType(), True),
        StructField("pickup_neighborhood", StringType(), True),
        StructField("pickup_district", StringType(), True),
        StructField("dropoff_neighborhood", StringType(), True),
        StructField("dropoff_district", StringType(), True),
        StructField("is_airport_trip", BooleanType(), False),
        StructField("is_downtown_trip", BooleanType(), False),
        StructField("processed_at", TimestampType(), False),
    ])

    GOLD_FACT_TRIPS_SPARK_SCHEMA = StructType([
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), True),
        StructField("company", StringType(), True),
        StructField("pickup_at", TimestampType(), False),
        StructField("dropoff_at", TimestampType(), False),
        StructField("pickup_date", StringType(), False),
        StructField("pickup_hour", IntegerType(), False),
        StructField("pickup_neighborhood", StringType(), True),
        StructField("pickup_district", StringType(), True),
        StructField("dropoff_neighborhood", StringType(), True),
        StructField("dropoff_district", StringType(), True),
        StructField("is_airport_trip", BooleanType(), False),
        StructField("is_downtown_trip", BooleanType(), False),
        StructField("distance_miles", DoubleType(), False),
        StructField("distance_km", DoubleType(), False),
        StructField("duration_minutes", DoubleType(), False),
        StructField("speed_kmh", DoubleType(), True),
        StructField("fare_amount", DoubleType(), False),
        StructField("tip_amount", DoubleType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("tip_percentage", DoubleType(), False),
        StructField("fare_per_km", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("is_raining", BooleanType(), False),
        StructField("curated_at", TimestampType(), False),
    ])
else:
    RAW_TAXI_SPARK_SCHEMA = None  # type: ignore
    RAW_WEATHER_SPARK_SCHEMA = None  # type: ignore
    SILVER_TAXI_SPARK_SCHEMA = None  # type: ignore
    GOLD_FACT_TRIPS_SPARK_SCHEMA = None  # type: ignore
