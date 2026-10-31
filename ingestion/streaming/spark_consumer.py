"""
==============================================================================
UrbanPulse - Spark Structured Streaming Taxi Consumer
==============================================================================
Consumes real-time taxi events from Kafka / AWS MSK (`sf-taxi-events`),
computes 5-minute tumbling window KPIs, and streams micro-batches to the
curated S3 Data Lake (`curated/streaming/metrics/`).

Architecture Role:
  Kafka MSK ──▶ Spark Structured Streaming ──▶ S3 Curated / Parquet Marts

Window Metrics Computed:
  - window_start, window_end
  - total_events, started_trips, completed_trips
  - total_fare_revenue, total_tips_collected, avg_fare
  - total_distance_miles, active_vehicles_count
  - trips_by_company aggregation

Dual Mode:
  - Spark Structured Streaming: When submitted via `spark-submit` on AWS EMR.
  - Local Python Streaming Evaluator: Simulates micro-batch sliding windows
    for local integration testing and verification without JVM dependencies.
==============================================================================
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.config.paths import to_local_path, to_s3_uri
from ingestion.config.s3_storage import StorageSink
from ingestion.config.settings import settings

logger = logging.getLogger("urbanpulse.spark_consumer")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [SparkStreaming] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def process_event_microbatch(
    events: List[Dict[str, Any]],
    window_duration_minutes: int = 5,
) -> List[Dict[str, Any]]:
    """
    Computes streaming window aggregations over a batch of taxi events.
    Groups events by tumbling window interval.
    """
    if not events:
        return []

    windows: Dict[str, Dict[str, Any]] = {}

    for evt in events:
        ts_str = evt.get("timestamp") or evt.get("pickup_datetime")
        try:
            ts = datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            ts = datetime.datetime.now(datetime.timezone.utc)

        # Calculate tumbling window bucket
        window_minutes = (ts.minute // window_duration_minutes) * window_duration_minutes
        window_start = ts.replace(minute=window_minutes, second=0, microsecond=0)
        window_end = window_start + datetime.timedelta(minutes=window_duration_minutes)
        window_key = window_start.strftime("%Y-%m-%dT%H:%M:%SZ")

        if window_key not in windows:
            windows[window_key] = {
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "total_events": 0,
                "started_trips": 0,
                "in_progress_events": 0,
                "completed_trips": 0,
                "total_fare_revenue": 0.0,
                "total_tips_collected": 0.0,
                "total_distance_miles": 0.0,
                "vehicles": set(),
                "companies": {},
            }

        w = windows[window_key]
        w["total_events"] += 1
        etype = evt.get("event_type", "UNKNOWN")

        if etype == "TRIP_STARTED":
            w["started_trips"] += 1
        elif etype == "TRIP_IN_PROGRESS":
            w["in_progress_events"] += 1
        elif etype == "TRIP_COMPLETED":
            w["completed_trips"] += 1
            w["total_fare_revenue"] += float(evt.get("fare_amount", 0.0))
            w["total_tips_collected"] += float(evt.get("tip_amount", 0.0))
            w["total_distance_miles"] += float(evt.get("distance_miles", 0.0))

        veh = evt.get("vehicle_id")
        if veh:
            w["vehicles"].add(veh)

        comp = evt.get("company", "Unknown")
        w["companies"][comp] = w["companies"].get(comp, 0) + 1

    # Format output records
    aggregated = []
    for k in sorted(windows.keys()):
        w = windows[k]
        completed = w["completed_trips"]
        avg_fare = round(w["total_fare_revenue"] / completed, 2) if completed > 0 else 0.0

        record = {
            "window_start": w["window_start"],
            "window_end": w["window_end"],
            "total_events": w["total_events"],
            "started_trips": w["started_trips"],
            "in_progress_events": w["in_progress_events"],
            "completed_trips": completed,
            "total_fare_revenue": round(w["total_fare_revenue"], 2),
            "total_tips_collected": round(w["total_tips_collected"], 2),
            "avg_fare": avg_fare,
            "total_distance_miles": round(w["total_distance_miles"], 2),
            "active_vehicles_count": len(w["vehicles"]),
            "company_distribution": w["companies"],
            "aggregated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        aggregated.append(record)

    return aggregated


def save_streaming_metrics(
    metrics: List[Dict[str, Any]],
    sink: Optional[StorageSink] = None,
) -> str:
    """Persists aggregated streaming micro-batch metrics to S3 Curated lake."""
    if not metrics:
        return ""

    if sink is None:
        sink = StorageSink()

    now = datetime.datetime.now(datetime.timezone.utc)
    s3_key = (
        f"curated/streaming/year={now.year}/month={now.month:02d}/"
        f"metrics_{now.strftime('%Y%m%d_%H%M%S')}.json"
    )

    data_payload = json.dumps(metrics, indent=2)
    sink.upload_bytes(
        data=data_payload.encode("utf-8"),
        s3_key=s3_key,
        content_type="application/json",
    )
    logger.info(f"Persisted {len(metrics)} streaming window metrics to {s3_key}")
    return s3_key


def run_spark_structured_streaming(
    bootstrap_servers: str,
    topic: str = "sf-taxi-events",
    checkpoint_location: str = "s3a://urbanpulse-lake/checkpoints/streaming",
    output_path: str = "s3a://urbanpulse-lake/curated/streaming",
) -> None:
    """
    Submits and runs a PySpark Structured Streaming job consuming Kafka MSK.
    Activated when run with PySpark installed on AWS EMR cluster.
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, from_json, to_timestamp, window
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError:
        logger.warning(
            "PySpark not installed. Use local micro-batch simulator or run via spark-submit on EMR."
        )
        return

    spark = (
        SparkSession.builder.appName("UrbanPulse-KafkaStreaming")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    schema = StructType([
        StructField("event_type", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("trip_id", StringType(), False),
        StructField("vehicle_id", StringType(), False),
        StructField("company", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("distance_miles", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ])

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "10 minutes")
    )

    windowed_kpis = (
        parsed_df.groupBy(window(col("event_time"), "5 minutes"), col("company"))
        .agg({
            "event_id": "count",
            "fare_amount": "sum",
            "distance_miles": "sum",
        })
    )

    query = (
        windowed_kpis.writeStream.format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output_path)
        .trigger(processingTime="1 minute")
        .start()
    )

    logger.info(f"Structured streaming query started on topic '{topic}'. Awaiting termination...")
    query.awaitTermination()


def main():
    parser = argparse.ArgumentParser(description="UrbanPulse Spark Streaming Taxi Consumer")
    parser.add_argument("--simulate", action="store_true", help="Run local simulated stream")
    parser.add_argument("--events-count", type=int, default=100, help="Number of events to simulate")
    parser.add_argument("--kafka-servers", type=str, default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", type=str, default="sf-taxi-events", help="Kafka topic")
    args = parser.parse_args()

    if args.simulate:
        from ingestion.streaming.event_producer import TaxiEventGenerator

        logger.info(f"Simulating {args.events_count} streaming taxi events...")
        gen = TaxiEventGenerator(fleet_size=20)
        events = [gen.generate_event() for _ in range(args.events_count)]

        metrics = process_event_microbatch(events, window_duration_minutes=5)
        logger.info(f"Computed {len(metrics)} tumbling windows.")
        for m in metrics:
            logger.info(
                f"Window {m['window_start']} -> {m['window_end']} | "
                f"Events: {m['total_events']} | Completed: {m['completed_trips']} | "
                f"Fare Revenue: ${m['total_fare_revenue']:.2f}"
            )
        save_streaming_metrics(metrics)
    else:
        run_spark_structured_streaming(bootstrap_servers=args.kafka_servers, topic=args.topic)


if __name__ == "__main__":
    main()
