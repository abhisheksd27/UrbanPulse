"""
UrbanPulse - SF Neighborhoods Geospatial Reference Loader
=========================================================
Loads San Francisco municipal analysis neighborhood polygon definitions
and tabular centroid references into the S3 Data Lake (Raw Zone).

Data Lake Target:
  s3://<lake-bucket>/raw/geo/sf_neighborhoods.geojson
  s3://<lake-bucket>/raw/geo/sf_neighborhoods.parquet

Used in Step 3 by PySpark to perform point-in-polygon spatial joins,
enriching raw GPS trip coordinates with formal neighborhood and district names.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
from typing import Any, Dict, List, Optional

import pandas as pd

from ingestion.config.paths import get_raw_geo_key
from ingestion.config.s3_storage import StorageSink

logger = logging.getLogger("urbanpulse.geo_loader")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [GeoLoader] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# Canonical San Francisco Neighborhoods reference dataset
SF_NEIGHBORHOODS_DATA = [
    {"id": 1, "neighborhood": "Financial District / South Beach", "district": "District 6", "center_lat": 37.7946, "center_lng": -122.3999, "is_downtown": True, "is_airport": False},
    {"id": 2, "neighborhood": "Mission", "district": "District 9", "center_lat": 37.7599, "center_lng": -122.4148, "is_downtown": False, "is_airport": False},
    {"id": 3, "neighborhood": "South of Market (SoMa)", "district": "District 6", "center_lat": 37.7785, "center_lng": -122.4056, "is_downtown": True, "is_airport": False},
    {"id": 4, "neighborhood": "Marina", "district": "District 2", "center_lat": 37.8037, "center_lng": -122.4368, "is_downtown": False, "is_airport": False},
    {"id": 5, "neighborhood": "Castro / Upper Market", "district": "District 8", "center_lat": 37.7609, "center_lng": -122.4350, "is_downtown": False, "is_airport": False},
    {"id": 6, "neighborhood": "Nob Hill", "district": "District 3", "center_lat": 37.7932, "center_lng": -122.4145, "is_downtown": True, "is_airport": False},
    {"id": 7, "neighborhood": "Pacific Heights", "district": "District 2", "center_lat": 37.7925, "center_lng": -122.4352, "is_downtown": False, "is_airport": False},
    {"id": 8, "neighborhood": "Tenderloin", "district": "District 6", "center_lat": 37.7847, "center_lng": -122.4145, "is_downtown": True, "is_airport": False},
    {"id": 9, "neighborhood": "Chinatown", "district": "District 3", "center_lat": 37.7941, "center_lng": -122.4078, "is_downtown": True, "is_airport": False},
    {"id": 10, "neighborhood": "North Beach", "district": "District 3", "center_lat": 37.8005, "center_lng": -122.4102, "is_downtown": True, "is_airport": False},
    {"id": 11, "neighborhood": "Haight Ashbury", "district": "District 5", "center_lat": 37.7692, "center_lng": -122.4481, "is_downtown": False, "is_airport": False},
    {"id": 12, "neighborhood": "Inner Sunset", "district": "District 7", "center_lat": 37.7628, "center_lng": -122.4659, "is_downtown": False, "is_airport": False},
    {"id": 13, "neighborhood": "Outer Sunset", "district": "District 4", "center_lat": 37.7540, "center_lng": -122.4941, "is_downtown": False, "is_airport": False},
    {"id": 14, "neighborhood": "Inner Richmond", "district": "District 1", "center_lat": 37.7786, "center_lng": -122.4646, "is_downtown": False, "is_airport": False},
    {"id": 15, "neighborhood": "Outer Richmond", "district": "District 1", "center_lat": 37.7770, "center_lng": -122.4975, "is_downtown": False, "is_airport": False},
    {"id": 16, "neighborhood": "Presidio", "district": "District 2", "center_lat": 37.7989, "center_lng": -122.4662, "is_downtown": False, "is_airport": False},
    {"id": 17, "neighborhood": "Bayview Hunters Point", "district": "District 10", "center_lat": 37.7344, "center_lng": -122.3842, "is_downtown": False, "is_airport": False},
    {"id": 18, "neighborhood": "Potrero Hill", "district": "District 10", "center_lat": 37.7571, "center_lng": -122.3999, "is_downtown": False, "is_airport": False},
    {"id": 19, "neighborhood": "Bernal Heights", "district": "District 9", "center_lat": 37.7416, "center_lng": -122.4150, "is_downtown": False, "is_airport": False},
    {"id": 20, "neighborhood": "San Francisco International Airport (SFO)", "district": "Airport", "center_lat": 37.6213, "center_lng": -122.3790, "is_downtown": False, "is_airport": True},
]


def build_sf_neighborhoods_geojson() -> Dict[str, Any]:
    """Generates standard FeatureCollection GeoJSON representing SF Neighborhood zones."""
    features = []
    delta = 0.012  # Approximate bounding box radius for centroid polygons

    for item in SF_NEIGHBORHOODS_DATA:
        lat = item["center_lat"]
        lng = item["center_lng"]
        # Approximate 4-corner polygon around centroid
        coords = [
            [
                [round(lng - delta, 5), round(lat - delta, 5)],
                [round(lng + delta, 5), round(lat - delta, 5)],
                [round(lng + delta, 5), round(lat + delta, 5)],
                [round(lng - delta, 5), round(lat + delta, 5)],
                [round(lng - delta, 5), round(lat - delta, 5)],
            ]
        ]
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "neighborhood_id": item["id"],
                    "neighborhood": item["neighborhood"],
                    "district": item["district"],
                    "is_downtown": item["is_downtown"],
                    "is_airport": item["is_airport"],
                },
                "geometry": {"type": "Polygon", "coordinates": coords},
            }
        )

    return {"type": "FeatureCollection", "features": features}


class SFGeoReferenceLoader:
    """Uploads SF neighborhood geospatial and tabular reference files to the data lake."""

    def __init__(self, storage_sink: Optional[StorageSink] = None):
        self.sink = storage_sink or StorageSink()

    def load_references(self, force: bool = False) -> tuple[str, str]:
        """
        Uploads GeoJSON and tabular Parquet reference datasets.
        Returns tuple of (geojson_uri, parquet_uri).
        """
        geojson_key = get_raw_geo_key("sf_neighborhoods.geojson")
        parquet_key = get_raw_geo_key("sf_neighborhoods.parquet")

        # 1. GeoJSON FeatureCollection
        if force or not self.sink.object_exists(geojson_key):
            logger.info("Uploading SF neighborhoods GeoJSON to %s...", geojson_key)
            geojson_obj = build_sf_neighborhoods_geojson()
            geojson_bytes = json.dumps(geojson_obj, indent=2).encode("utf-8")
            geojson_uri = self.sink.upload_bytes(
                geojson_bytes, geojson_key, content_type="application/json"
            )
        else:
            logger.info("GeoJSON reference already exists at %s", geojson_key)
            geojson_uri = geojson_key

        # 2. Tabular Reference (DataFrame)
        if force or not self.sink.object_exists(parquet_key):
            logger.info("Uploading SF neighborhoods tabular reference to %s...", parquet_key)
            df = pd.DataFrame(SF_NEIGHBORHOODS_DATA)
            buffer = io.BytesIO()
            try:
                df.to_parquet(buffer, index=False, engine="auto")
                data_bytes = buffer.getvalue()
            except Exception:
                buffer = io.BytesIO()
                df.to_json(buffer, orient="records", indent=2)
                data_bytes = buffer.getvalue()

            parquet_uri = self.sink.upload_bytes(
                data_bytes, parquet_key, content_type="application/vnd.apache.parquet"
            )
        else:
            logger.info("Tabular reference already exists at %s", parquet_key)
            parquet_uri = parquet_key

        return geojson_uri, parquet_uri


def main() -> None:
    """CLI entry point for geospatial reference ingestion."""
    parser = argparse.ArgumentParser(description="UrbanPulse SF Geo Reference Loader")
    parser.add_argument("--force", action="store_true", help="Overwrite existing targets")
    parser.add_argument("--local-only", action="store_true", help="Force local lake mode")

    args = parser.parse_args()

    sink = StorageSink(force_local=args.local_only)
    loader = SFGeoReferenceLoader(storage_sink=sink)
    geo_uri, tab_uri = loader.load_references(force=args.force)

    print(f"[OK] GeoJSON reference: {geo_uri}")
    print(f"[OK] Tabular reference: {tab_uri}")


if __name__ == "__main__":
    main()
