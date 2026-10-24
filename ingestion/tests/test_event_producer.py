"""Unit tests for Kafka telemetry event generator."""

import unittest
from ingestion.streaming.event_producer import TaxiEventGenerator


class TestEventProducer(unittest.TestCase):

    def test_event_generation(self):
        generator = TaxiEventGenerator(fleet_size=10)

        # Generate a batch of events
        events = [generator.generate_event() for _ in range(30)]

        event_types = {e["event_type"] for e in events}
        self.assertIn("TRIP_STARTED", event_types)

        # Validate event properties
        for evt in events:
            self.assertIn("event_id", evt)
            self.assertIn("trip_id", evt)
            self.assertIn("timestamp", evt)
            self.assertIn("status", evt)


if __name__ == "__main__":
    unittest.main()
