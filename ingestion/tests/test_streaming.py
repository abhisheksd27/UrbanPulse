"""
Unit tests for Spark Structured Streaming taxi consumer and window aggregations.
"""

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from ingestion.config.s3_storage import StorageSink
from ingestion.streaming.spark_consumer import (
    process_event_microbatch,
    save_streaming_metrics,
)


class TestStreamingConsumer(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.sink = StorageSink(force_local=True)
        self.sink.local_base_dir = Path(self.tmp_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_process_event_microbatch_empty(self):
        """Ensures empty batch returns empty list."""
        self.assertEqual(process_event_microbatch([]), [])

    def test_process_event_microbatch_aggregations(self):
        """Validates tumbling window aggregation logic across multiple event types."""
        events = [
            {
                "event_type": "TRIP_STARTED",
                "event_id": "EVT-1",
                "trip_id": "T-100",
                "vehicle_id": "V-10",
                "company": "Yellow Cab SF",
                "timestamp": "2024-01-15T10:01:00Z",
            },
            {
                "event_type": "TRIP_IN_PROGRESS",
                "event_id": "EVT-2",
                "trip_id": "T-100",
                "vehicle_id": "V-10",
                "company": "Yellow Cab SF",
                "timestamp": "2024-01-15T10:03:00Z",
            },
            {
                "event_type": "TRIP_COMPLETED",
                "event_id": "EVT-3",
                "trip_id": "T-100",
                "vehicle_id": "V-10",
                "company": "Yellow Cab SF",
                "timestamp": "2024-01-15T10:04:30Z",
                "fare_amount": 25.50,
                "tip_amount": 4.50,
                "distance_miles": 5.2,
            },
            {
                "event_type": "TRIP_COMPLETED",
                "event_id": "EVT-4",
                "trip_id": "T-200",
                "vehicle_id": "V-20",
                "company": "Flywheel Taxi",
                "timestamp": "2024-01-15T10:04:45Z",
                "fare_amount": 15.00,
                "tip_amount": 3.00,
                "distance_miles": 2.8,
            },
            # Second 5-minute window event
            {
                "event_type": "TRIP_STARTED",
                "event_id": "EVT-5",
                "trip_id": "T-300",
                "vehicle_id": "V-30",
                "company": "SF Green Cab",
                "timestamp": "2024-01-15T10:07:00Z",
            },
        ]

        metrics = process_event_microbatch(events, window_duration_minutes=5)
        self.assertEqual(len(metrics), 2)

        # Inspect Window 1 (10:00 - 10:05)
        w1 = metrics[0]
        self.assertIn("2024-01-15T10:00:00", w1["window_start"])
        self.assertEqual(w1["total_events"], 4)
        self.assertEqual(w1["started_trips"], 1)
        self.assertEqual(w1["in_progress_events"], 1)
        self.assertEqual(w1["completed_trips"], 2)
        self.assertEqual(w1["total_fare_revenue"], 40.50)
        self.assertEqual(w1["total_tips_collected"], 7.50)
        self.assertEqual(w1["avg_fare"], 20.25)
        self.assertEqual(w1["total_distance_miles"], 8.0)
        self.assertEqual(w1["active_vehicles_count"], 2)
        self.assertEqual(w1["company_distribution"]["Yellow Cab SF"], 3)
        self.assertEqual(w1["company_distribution"]["Flywheel Taxi"], 1)

        # Inspect Window 2 (10:05 - 10:10)
        w2 = metrics[1]
        self.assertIn("2024-01-15T10:05:00", w2["window_start"])
        self.assertEqual(w2["total_events"], 1)
        self.assertEqual(w2["started_trips"], 1)
        self.assertEqual(w2["completed_trips"], 0)

    def test_save_streaming_metrics(self):
        """Verifies persistence of window metrics to storage sink."""
        sample_metrics = [
            {
                "window_start": "2024-01-15T10:00:00Z",
                "window_end": "2024-01-15T10:05:00Z",
                "total_events": 10,
                "completed_trips": 2,
                "total_fare_revenue": 50.0,
            }
        ]

        s3_key = save_streaming_metrics(sample_metrics, sink=self.sink)
        self.assertTrue(s3_key.startswith("curated/streaming/"))
        self.assertTrue(self.sink.object_exists(s3_key))

        content = self.sink.read_bytes(s3_key)
        loaded = json.loads(content.decode("utf-8"))
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0]["total_fare_revenue"], 50.0)


if __name__ == "__main__":
    unittest.main()
