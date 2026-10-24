"""Unit tests for SF Taxi batch loader."""

import unittest
from ingestion.batch.sf_taxi_loader import (
    calculate_month_date_range,
    generate_synthetic_sf_taxi_batch,
    normalize_taxi_records,
)


class TestSFTaxiLoader(unittest.TestCase):

    def test_date_range_calculation(self):
        start, end = calculate_month_date_range(2024, 1)
        self.assertEqual(start, "2024-01-01T00:00:00")
        self.assertEqual(end, "2024-02-01T00:00:00")

        # December boundary
        start_dec, end_dec = calculate_month_date_range(2024, 12)
        self.assertEqual(start_dec, "2024-12-01T00:00:00")
        self.assertEqual(end_dec, "2025-01-01T00:00:00")

    def test_synthetic_generation(self):
        records = generate_synthetic_sf_taxi_batch(2024, 1, num_records=25)
        self.assertEqual(len(records), 25)

        first = records[0]
        self.assertIn("trip_id", first)
        self.assertIn("pickup_latitude", first)
        self.assertIn("fare", first)
        self.assertGreater(first["fare"], 0.0)

    def test_normalization(self):
        raw = [
            {
                "riderid": "SF-TEST-1",
                "medallion": "CAB-99",
                "company": "Flywheel",
                "start_time": "2024-01-15T12:00:00",
                "end_time": "2024-01-15T12:20:00",
                "pickup_lat": "37.7749",
                "pickup_lon": "-122.4194",
                "dropoff_lat": "37.7849",
                "dropoff_lon": "-122.4094",
                "fare": "18.50",
                "tips": "3.50",
                "distance": "3.2",
                "payment_type": "Credit Card",
            }
        ]
        df = normalize_taxi_records(raw)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["trip_id"].iloc[0], "SF-TEST-1")
        self.assertEqual(df["vehicle_id"].iloc[0], "CAB-99")
        self.assertEqual(df["fare"].iloc[0], 18.50)
        self.assertEqual(df["distance"].iloc[0], 3.2)


if __name__ == "__main__":
    unittest.main()
