"""Unit tests for Silver to Gold PySpark curation logic."""

import unittest
import pandas as pd
from spark.jobs.silver_to_gold import (
    compute_daily_ops_mart,
    enrich_and_curate_trips,
    translate_weather_code,
)


class TestSilverToGold(unittest.TestCase):

    def test_translate_weather_code(self):
        self.assertEqual(translate_weather_code(0), "Clear Sky")
        self.assertEqual(translate_weather_code(45), "Fog")
        self.assertEqual(translate_weather_code(61), "Slight Rain")

    def test_enrich_and_curate_trips(self):
        silver_data = [
            {
                "trip_id": "T1",
                "pickup_at": "2024-01-01T14:15:00Z",
                "dropoff_at": "2024-01-01T14:45:00Z",
                "pickup_date": "2024-01-01",
                "pickup_hour": 14,
                "pickup_neighborhood": "Mission",
                "pickup_district": "District 9",
                "dropoff_neighborhood": "Downtown",
                "dropoff_district": "District 6",
                "is_airport_trip": False,
                "is_downtown_trip": True,
                "distance_miles": 5.0,
                "distance_km": 8.047,
                "duration_minutes": 30.0,
                "fare_amount": 20.0,
                "tip_amount": 4.0,
                "total_amount": 24.0,
                "payment_type": "Credit Card",
            }
        ]
        weather_data = [
            {
                "time": "2024-01-01T14:00:00Z",
                "temperature_2m": 16.5,
                "precipitation": 0.0,
                "windspeed_10m": 12.0,
                "visibility": 10000,
                "weathercode": 0,
            }
        ]
        df_silver = pd.DataFrame(silver_data)
        df_weather = pd.DataFrame(weather_data)

        df_gold = enrich_and_curate_trips(df_silver, df_weather)

        self.assertEqual(len(df_gold), 1)
        self.assertEqual(df_gold["temperature_c"].iloc[0], 16.5)
        self.assertEqual(df_gold["weather_condition"].iloc[0], "Clear Sky")
        self.assertEqual(df_gold["tip_percentage"].iloc[0], 20.0)
        # speed = 8.047 km / 0.5 hours = 16.09 km/h
        self.assertAlmostEqual(df_gold["speed_kmh"].iloc[0], 16.09, places=1)

    def test_compute_daily_ops_mart(self):
        curated_data = [
            {
                "trip_id": "T1",
                "pickup_date": "2024-01-01",
                "pickup_neighborhood": "Mission",
                "total_amount": 24.0,
                "fare_amount": 20.0,
                "tip_amount": 4.0,
                "duration_minutes": 30.0,
                "distance_km": 8.0,
                "speed_kmh": 16.0,
                "is_raining": False,
                "is_airport_trip": False,
            },
            {
                "trip_id": "T2",
                "pickup_date": "2024-01-01",
                "pickup_neighborhood": "Mission",
                "total_amount": 36.0,
                "fare_amount": 30.0,
                "tip_amount": 6.0,
                "duration_minutes": 40.0,
                "distance_km": 12.0,
                "speed_kmh": 18.0,
                "is_raining": True,
                "is_airport_trip": False,
            },
        ]
        df_curated = pd.DataFrame(curated_data)
        df_mart = compute_daily_ops_mart(df_curated)

        self.assertEqual(len(df_mart), 1)
        self.assertEqual(df_mart["total_trips"].iloc[0], 2)
        self.assertEqual(df_mart["total_revenue"].iloc[0], 60.0)
        self.assertEqual(df_mart["avg_fare"].iloc[0], 25.0)
        self.assertEqual(df_mart["rainy_trips"].iloc[0], 1)


if __name__ == "__main__":
    unittest.main()
