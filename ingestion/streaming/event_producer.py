"""
UrbanPulse - Real-time SF Taxi Telemetry Event Producer
======================================================
Simulates real-time GPS telemetry and metering events from active taxi fleets
across San Francisco and publishes them to an Apache Kafka / AWS MSK topic.

Architecture Role:
  Python Event Producer ──▶ Kafka MSK (sf-taxi-events) ──▶ Spark Structured Streaming

Events Produced:
  - TRIP_STARTED: Initial meter start with pickup coordinates and time
  - TRIP_IN_PROGRESS: In-route GPS breadcrumb with active distance and speed
  - TRIP_COMPLETED: Final trip summary with total fare, tip, and duration
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import random
import sys
import time
from typing import Any, Dict, Generator, Optional

from ingestion.config.settings import settings

logger = logging.getLogger("urbanpulse.event_producer")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [KafkaProducer] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# SF Landmarks for realistic telemetry origin/destination clusters
SF_HUBS = [
    {"name": "SFO Airport", "lat": 37.6213, "lng": -122.3790},
    {"name": "Financial District", "lat": 37.7946, "lng": -122.3999},
    {"name": "Moscone Center / SoMa", "lat": 37.7842, "lng": -122.4016},
    {"name": "Mission District", "lat": 37.7599, "lng": -122.4148},
    {"name": "Fisherman's Wharf", "lat": 37.8080, "lng": -122.4177},
    {"name": "Union Square", "lat": 37.7879, "lng": -122.4074},
    {"name": "Castro", "lat": 37.7609, "lng": -122.4350},
    {"name": "Marina District", "lat": 37.8037, "lng": -122.4368},
    {"name": "Civic Center / City Hall", "lat": 37.7793, "lng": -122.4193},
]

COMPANIES = [
    "Yellow Cab SF",
    "Flywheel Taxi",
    "SF Green Cab",
    "National Cab",
    "Town Taxi",
]


class TaxiEventGenerator:
    """Generates continuous streams of realistic taxi GPS events."""

    def __init__(self, fleet_size: int = 50):
        self.fleet_size = fleet_size
        self.active_trips: Dict[str, Dict[str, Any]] = {}
        self.trip_counter = 1000

    def generate_event(self) -> Dict[str, Any]:
        """Generate the next discrete telemetry event."""
        now = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()

        # Decide whether to advance existing trip or start new one
        if self.active_trips and random.random() < 0.65:
            # Advance or complete an active trip
            trip_id = random.choice(list(self.active_trips.keys()))
            trip = self.active_trips[trip_id]
            trip["step"] += 1

            # Interpolate towards destination
            total_steps = trip["total_steps"]
            progress = min(1.0, trip["step"] / total_steps)

            curr_lat = round(
                trip["pickup_lat"] + progress * (trip["dropoff_lat"] - trip["pickup_lat"])
                + random.uniform(-0.001, 0.001),
                6,
            )
            curr_lng = round(
                trip["pickup_lng"] + progress * (trip["dropoff_lng"] - trip["pickup_lng"])
                + random.uniform(-0.001, 0.001),
                6,
            )
            curr_distance = round(trip["target_distance"] * progress, 2)
            curr_fare = round(3.50 + curr_distance * 3.25, 2)

            if progress >= 1.0 or trip["step"] >= total_steps:
                # TRIP COMPLETED
                self.active_trips.pop(trip_id, None)
                tips = round(curr_fare * 0.18, 2) if random.random() > 0.3 else 0.0
                return {
                    "event_type": "TRIP_COMPLETED",
                    "event_id": f"EVT-{int(now.timestamp() * 1000)}",
                    "trip_id": trip_id,
                    "vehicle_id": trip["vehicle_id"],
                    "company": trip["company"],
                    "timestamp": now_iso,
                    "pickup_datetime": trip["start_time"],
                    "dropoff_datetime": now_iso,
                    "pickup_latitude": trip["pickup_lat"],
                    "pickup_longitude": trip["pickup_lng"],
                    "dropoff_latitude": trip["dropoff_lat"],
                    "dropoff_longitude": trip["dropoff_lng"],
                    "distance_miles": curr_distance,
                    "fare_amount": curr_fare,
                    "tip_amount": tips,
                    "total_amount": round(curr_fare + tips, 2),
                    "payment_type": trip["payment_type"],
                    "status": "COMPLETED",
                }
            else:
                # TRIP IN PROGRESS
                speed_mph = round(random.uniform(15.0, 42.0), 1)
                return {
                    "event_type": "TRIP_IN_PROGRESS",
                    "event_id": f"EVT-{int(now.timestamp() * 1000)}",
                    "trip_id": trip_id,
                    "vehicle_id": trip["vehicle_id"],
                    "company": trip["company"],
                    "timestamp": now_iso,
                    "current_latitude": curr_lat,
                    "current_longitude": curr_lng,
                    "speed_mph": speed_mph,
                    "distance_so_far_miles": curr_distance,
                    "meter_fare_so_far": curr_fare,
                    "progress_pct": round(progress * 100, 1),
                    "status": "EN_ROUTE",
                }
        else:
            # START A NEW TRIP
            self.trip_counter += 1
            trip_id = f"LIVE-SF-{self.trip_counter}"
            vehicle_id = f"CAB-{random.randint(100, 100 + self.fleet_size)}"
            company = random.choice(COMPANIES)

            origin = random.choice(SF_HUBS)
            destination = random.choice([h for h in SF_HUBS if h != origin])

            p_lat = round(origin["lat"] + random.uniform(-0.005, 0.005), 6)
            p_lng = round(origin["lng"] + random.uniform(-0.005, 0.005), 6)
            d_lat = round(destination["lat"] + random.uniform(-0.005, 0.005), 6)
            d_lng = round(destination["lng"] + random.uniform(-0.005, 0.005), 6)

            # Approx distance
            lat_diff = (d_lat - p_lat) * 69.0
            lng_diff = (d_lng - p_lng) * 54.6
            target_distance = max(1.2, round((lat_diff**2 + lng_diff**2) ** 0.5, 2))
            total_steps = random.randint(4, 12)

            self.active_trips[trip_id] = {
                "vehicle_id": vehicle_id,
                "company": company,
                "start_time": now_iso,
                "pickup_lat": p_lat,
                "pickup_lng": p_lng,
                "dropoff_lat": d_lat,
                "dropoff_lng": d_lng,
                "target_distance": target_distance,
                "step": 0,
                "total_steps": total_steps,
                "payment_type": random.choice(["Credit Card", "Cash", "Mobile Pay"]),
            }

            return {
                "event_type": "TRIP_STARTED",
                "event_id": f"EVT-{int(now.timestamp() * 1000)}",
                "trip_id": trip_id,
                "vehicle_id": vehicle_id,
                "company": company,
                "timestamp": now_iso,
                "pickup_latitude": p_lat,
                "pickup_longitude": p_lng,
                "target_dropoff_latitude": d_lat,
                "target_dropoff_longitude": d_lng,
                "estimated_distance_miles": target_distance,
                "base_fare": 3.50,
                "status": "STARTED",
            }


class KafkaEventPublisher:
    """Publishes telemetry events to Kafka MSK or streams to stdout."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self.topic = topic or settings.kafka_taxi_topic
        self.dry_run = dry_run
        self._producer = None

        if not dry_run:
            self._connect_kafka()

    def _connect_kafka(self) -> None:
        """Connect to Apache Kafka broker cluster."""
        try:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                request_timeout_ms=10000,
            )
            logger.info(
                "Connected to Kafka brokers at %s (Topic: %s)",
                self.bootstrap_servers,
                self.topic,
            )
        except (ImportError, Exception) as exc:
            logger.warning(
                "Kafka broker not reachable or kafka-python missing (%s). Running in dry-run simulation mode.",
                exc,
            )
            self.dry_run = True

    def publish_event(self, event: Dict[str, Any]) -> None:
        """Publish a single event to Kafka or print."""
        key = event.get("trip_id", "")
        if not self.dry_run and self._producer:
            try:
                self._producer.send(self.topic, key=key, value=event)
                logger.debug("Published event [%s] trip_id=%s", event["event_type"], key)
            except Exception as exc:
                logger.error("Failed to publish event to Kafka: %s", exc)
        else:
            logger.info("SIMULATED EVENT: [%s] trip_id=%s -> %s", event["event_type"], key, json.dumps(event))

    def flush(self) -> None:
        """Flush pending buffered messages."""
        if self._producer:
            self._producer.flush()


def run_producer_loop(
    rate_hz: float = 2.0,
    max_events: Optional[int] = None,
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Run continuous publishing loop at given frequency (events/second)."""
    publisher = KafkaEventPublisher(
        bootstrap_servers=bootstrap_servers, topic=topic, dry_run=dry_run
    )
    generator = TaxiEventGenerator()

    interval = 1.0 / max(0.1, rate_hz)
    count = 0

    logger.info("Starting Taxi Event Producer loop (Rate: %.1f events/sec)...", rate_hz)

    try:
        while True:
            event = generator.generate_event()
            publisher.publish_event(event)
            count += 1

            if max_events and count >= max_events:
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Producer loop stopped by user.")
    finally:
        publisher.flush()

    logger.info("Event Producer finished. Total events emitted: %d", count)
    return count


def main() -> None:
    """CLI entry point for running the event producer."""
    parser = argparse.ArgumentParser(description="UrbanPulse Real-time Taxi Event Producer")
    parser.add_argument("--rate", type=float, default=2.0, help="Publish rate in events/second")
    parser.add_argument("--max-events", type=int, default=20, help="Max events to generate before exiting")
    parser.add_argument("--broker", type=str, default=None, help="Kafka bootstrap broker list")
    parser.add_argument("--topic", type=str, default=None, help="Kafka topic name")
    parser.add_argument("--dry-run", action="store_true", help="Print events to stdout without connecting to Kafka")

    args = parser.parse_args()

    run_producer_loop(
        rate_hz=args.rate,
        max_events=args.max_events,
        bootstrap_servers=args.broker,
        topic=args.topic,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
