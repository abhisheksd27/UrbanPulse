# ==============================================================================
# spark/jobs/__init__.py
#
# Package: spark.jobs
#
# Modules:
#   bronze_to_silver.py   — PySpark job: Raw zone → Staging zone
#                           Applies schema, cleans, deduplicates, spatially enriches
#   silver_to_gold.py     — PySpark job: Staging zone → Curated zone
#                           Joins with weather, computes metrics, writes partitioned Parquet
#   schema_definitions.py — All PySpark StructType schemas for every dataset
#                           Centralised to prevent schema drift across jobs
# ==============================================================================
