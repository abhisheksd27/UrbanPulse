# ADR-001: Storage Format — Parquet over CSV

**Date**: 2024-10-24
**Status**: Accepted
**Deciders**: UrbanPulse Data Engineering Team

---

## Context

We need to choose a file format for storing data in the S3 Data Lake across all three zones (raw, staging, curated).

## Decision

Use **Apache Parquet** for all data stored in S3.

## Consequences

### Positive
- **Columnar storage** — analytics queries read only the columns they need, not full rows. For a 30-column trip record where most queries use 5-8 columns, this gives 4-6x faster scans.
- **Compression** — Parquet + Snappy codec reduces file sizes by ~75% vs raw CSV, directly cutting S3 storage costs.
- **Embedded schema** — data types are stored in the file metadata. No ambiguity about whether `fare_amount` is a string or decimal.
- **Native support** — Spark, Redshift COPY, Athena, and pandas all read Parquet natively with zero conversion.
- **Partition pruning** — when writing Parquet partitioned by `year/month`, query engines skip irrelevant partitions entirely.

### Negative
- **Not human-readable** — engineers can't open a Parquet file in a text editor. Use `parquet-tools` or `pandas.read_parquet` for inspection.
- **Small files problem** — if ingestion produces many tiny files, we need to use Spark's `coalesce()` to combine them before writing.

## Alternatives Considered

| Format | Rejected Because |
|---|---|
| CSV | No schema, slow column reads, no compression |
| JSON | Row-oriented, verbose, slow for analytics |
| ORC | Parquet has better ecosystem support in Python |
| Delta Lake | Adds operational complexity; revisit in v2 |
