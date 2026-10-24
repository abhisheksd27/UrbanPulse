"""Unit tests for lake path conventions."""

import unittest
from ingestion.config.paths import (
    format_year_month,
    get_curated_taxi_prefix,
    get_raw_geo_key,
    get_raw_taxi_key,
    get_raw_weather_key,
    get_staging_taxi_prefix,
    to_s3_uri,
)


class TestLakePaths(unittest.TestCase):

    def test_format_year_month(self):
        y, m = format_year_month(2024, 1)
        self.assertEqual(y, "2024")
        self.assertEqual(m, "01")

        y, m = format_year_month("2023", "12")
        self.assertEqual(y, "2023")
        self.assertEqual(m, "12")

    def test_get_raw_taxi_key(self):
        key = get_raw_taxi_key(2024, 3)
        self.assertEqual(key, "raw/taxi/year=2024/month=03/trips.parquet")

    def test_get_raw_weather_key(self):
        key = get_raw_weather_key(2024, 5)
        self.assertEqual(key, "raw/weather/year=2024/month=05/weather.parquet")

    def test_get_raw_geo_key(self):
        key = get_raw_geo_key()
        self.assertEqual(key, "raw/geo/sf_neighborhoods.geojson")

    def test_staging_and_curated_prefixes(self):
        stg = get_staging_taxi_prefix(2024, 1)
        self.assertEqual(stg, "staging/taxi/year=2024/month=01/")

        cur = get_curated_taxi_prefix(2024, 1)
        self.assertEqual(cur, "curated/taxi/year=2024/month=01/")

    def test_to_s3_uri(self):
        uri = to_s3_uri("my-bucket", "raw/taxi/test.parquet")
        self.assertEqual(uri, "s3://my-bucket/raw/taxi/test.parquet")


if __name__ == "__main__":
    unittest.main()
