"""
UrbanPulse Batch Ingestion Package
==================================
Modules:
  - sf_taxi_loader: Downloads and lands SF taxi trip Parquet data
  - weather_ingester: Fetches and lands Open-Meteo hourly weather Parquet data
  - geo_loader: Loads SF neighborhoods GeoJSON and tabular reference lookups
"""

from ingestion.batch.geo_loader import SFGeoReferenceLoader
from ingestion.batch.sf_taxi_loader import SFTaxiBatchLoader
from ingestion.batch.weather_ingester import SFWeatherBatchIngester

__all__ = [
    "SFTaxiBatchLoader",
    "SFWeatherBatchIngester",
    "SFGeoReferenceLoader",
]
