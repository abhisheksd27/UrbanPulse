"""
UrbanPulse Streaming Ingestion Package
======================================
Modules:
  - event_producer: Real-time SF taxi GPS and meter telemetry event generator
"""

from ingestion.streaming.event_producer import (
    KafkaEventPublisher,
    TaxiEventGenerator,
    run_producer_loop,
)

__all__ = [
    "TaxiEventGenerator",
    "KafkaEventPublisher",
    "run_producer_loop",
]
