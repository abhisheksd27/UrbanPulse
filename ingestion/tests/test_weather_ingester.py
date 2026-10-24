"""Unit tests for SF Weather ingester."""

import unittest
from ingestion.batch.weather_ingester import (
    generate_synthetic_sf_weather,
    parse_openmeteo_response,
)


class TestSFWeatherIngester(unittest.TestCase):

    def test_synthetic_weather_generation(self):
        df = generate_synthetic_sf_weather(2024, 1)
        # January has 31 days * 24 hours = 744 observations
        self.assertEqual(len(df), 744)
        self.assertIn("temperature_2m", df.columns)
        self.assertIn("precipitation", df.columns)
        self.assertIn("weathercode", df.columns)

    def test_parse_openmeteo_response(self):
        mock_payload = {
            "hourly": {
                "time": ["2024-01-01T00:00", "2024-01-01T01:00"],
                "temperature_2m": [11.2, 10.8],
                "precipitation": [0.0, 0.4],
                "windspeed_10m": [14.5, 16.2],
                "visibility": [10000, 8500],
                "weathercode": [0, 61],
            }
        }
        df = parse_openmeteo_response(mock_payload)
        self.assertEqual(len(df), 2)
        self.assertEqual(df["temperature_2m"].iloc[0], 11.2)
        self.assertEqual(df["weathercode"].iloc[1], 61)


if __name__ == "__main__":
    unittest.main()
