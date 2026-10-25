"""Unit tests for Bronze to Silver PySpark transformation logic."""

import unittest
import pandas as pd
from spark.jobs.bronze_to_silver import (
    clean_and_transform_taxi_dataframe,
    find_nearest_neighborhood,
)


class TestBronzeToSilver(unittest.TestCase):

    def test_find_nearest_neighborhood(self):
        # SFO Airport coords
        name, district, is_dt, is_air = find_nearest_neighborhood(37.6213, -122.3790)
        self.assertIn("Airport", name)
        self.assertTrue(is_air)

        # Financial District coords
        name_f, _, is_dt_f, is_air_f = find_nearest_neighborhood(37.7946, -122.3999)
        self.assertIn("Financial District", name_f)
        self.assertTrue(is_dt_f)
        self.assertFalse(is_air_f)

    def test_cleaning_and_deduplication(self):
        raw_data = [
            {
                "trip_id": "T1",
                "pickup_datetime": "2024-01-01T10:00:00Z",
                "dropoff_datetime": "2024-01-01T10:20:00Z",
                "pickup_latitude": 37.7749,
                "pickup_longitude": -122.4194,
                "dropoff_latitude": 37.7849,
                "dropoff_longitude": -122.4094,
                "fare": 15.0,
                "tips": 3.0,
                "distance": 2.5,
                "payment_type": "Credit Card",
            },
            # Exact duplicate trip_id (should be dropped)
            {
                "trip_id": "T1",
                "pickup_datetime": "2024-01-01T10:00:00Z",
                "dropoff_datetime": "2024-01-01T10:20:00Z",
                "pickup_latitude": 37.7749,
                "pickup_longitude": -122.4194,
                "dropoff_latitude": 37.7849,
                "dropoff_longitude": -122.4094,
                "fare": 15.0,
                "tips": 3.0,
                "distance": 2.5,
                "payment_type": "Credit Card",
            },
            # Invalid trip: negative fare (should be dropped)
            {
                "trip_id": "T2_INVALID",
                "pickup_datetime": "2024-01-01T11:00:00Z",
                "dropoff_datetime": "2024-01-01T11:15:00Z",
                "pickup_latitude": 37.7749,
                "pickup_longitude": -122.4194,
                "dropoff_latitude": 37.7849,
                "dropoff_longitude": -122.4094,
                "fare": -5.0,
                "tips": 0.0,
                "distance": 1.0,
                "payment_type": "Cash",
            },
        ]
        df_raw = pd.DataFrame(raw_data)
        df_silver = clean_and_transform_taxi_dataframe(df_raw)

        # Only 1 record should survive (T1)
        self.assertEqual(len(df_silver), 1)
        self.assertEqual(df_silver["trip_id"].iloc[0], "T1")
        self.assertEqual(df_silver["duration_minutes"].iloc[0], 20.0)
        self.assertEqual(df_silver["total_amount"].iloc[0], 18.0)
        self.assertAlmostEqual(df_silver["distance_km"].iloc[0], 4.023, places=2)
        self.assertIn("pickup_neighborhood", df_silver.columns)


if __name__ == "__main__":
    unittest.main()
