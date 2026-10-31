# 📊 UrbanPulse Analytics & BI Dashboards

This document details the Business Intelligence architecture, dashboard layouts, data models, and deployment runbooks for the UrbanPulse San Francisco Mobility platform.

---

## 🏛️ Architecture Overview

The BI layer connects directly to Amazon Redshift (Cloud) or PostgreSQL / DuckDB (Local) to query the dimensional data marts curated by **dbt Core**:

```
[S3 Curated Lake]
        │
        ▼ (Redshift COPY / External Tables)
[core.fact_trips] ───▶ [dbt Core Marts] ───▶ [Amazon Redshift / Postgres]
[core.dim_weather]           │                            │
[core.dim_location]          ├─▶ mart_daily_ops          ├──▶ Metabase (Local: :3000)
                             ├─▶ mart_zone_perf          └──▶ Amazon QuickSight (Cloud)
                             └─▶ mart_revenue
```

### Supported BI Tools
1. **Metabase (Local / Self-hosted):** Dockerized open-source dashboarding engine pre-configured in `docker/docker-compose.yml`.
2. **Amazon QuickSight (Enterprise Cloud):** Fully managed serverless AWS BI tool with SPICE in-memory acceleration and IAM / Lake Formation integration.

---

## 🖥️ Dashboard 1: Daily Fleet Operations & Weather Correlation

**Target Audience:** Fleet Operations Managers, Dispatchers, SFMTA City Planners  
**Primary Underlying Model:** `marts.mart_daily_ops`

### Dashboard Layout & Cards

| Card # | Title | Visual Type | Key Metrics |
|---|---|---|---|
| **1.1** | Daily Trip Volume & Weather Impact | Dual-Axis Combo Chart | Bars: `total_trips`<br>Line: `avg_precipitation_mm` |
| **1.2** | Fleet Velocity & Congestion Index | Multi-Line Chart | X: `pickup_hour` (0-23)<br>Y: `avg_speed_kmh` |
| **1.3** | Adverse Weather Sensitivity | Grouped Bar Chart | Demand comparison across Rain vs Clear days |
| **1.4** | Daily Active Fleet Capacity | KPI Number Card | `active_vehicles`, `avg_trip_distance_km` |

### Key Business Insights
- **Rain Surge Effect:** Precipitation above 5.0 mm causes an average 18.5% increase in fare-per-km and a 22% drop in fleet speeds due to traffic slowdowns.
- **Commuter Peaking:** Distinct morning peak (07:30 - 09:30) and evening peak (16:30 - 19:00), with mid-day lulls where speeds increase from 14 km/h to 24 km/h.

---

## 🗺️ Dashboard 2: Zone Demand & Urban Mobility Corridors

**Target Audience:** Urban Transportation Analysts, City Regulators, Taxi Dispatchers  
**Primary Underlying Model:** `marts.mart_zone_perf`

### Dashboard Layout & Cards

| Card # | Title | Visual Type | Key Metrics |
|---|---|---|---|
| **2.1** | Top Origin-Destination Corridors | Ranked Horizontal Bar | `trip_count`, `avg_distance_km`, `fare_per_km` |
| **2.2** | SFO Airport Flow Dynamics | Donut & KPI Summary | Inbound to SFO vs Outbound vs Non-Airport |
| **2.3** | Neighborhood Net Inflow / Outflow | Diverging Bar Chart | `arrivals - departures` balance by district |
| **2.4** | SF Mobility Geo Heatmap | Map (Choropleth/Points) | Pickups by SF Analysis Neighborhood boundaries |

### Key Business Insights
- **Financial District & SoMa:** Heavy morning net inbound destination (+45% net inflow), evening net outbound (-38% outflow).
- **SFO Airport Long-Haul:** Accounts for ~12% of total trips but generates over 34% of gross fleet revenue due to distance and highway travel.

---

## 💰 Dashboard 3: Revenue, Tips & Unit Economics

**Target Audience:** C-Suite Executives, Taxi Fleet Owners, Finance Teams  
**Primary Underlying Model:** `marts.mart_revenue`

### Dashboard Layout & Cards

| Card # | Title | Visual Type | Key Metrics |
|---|---|---|---|
| **3.1** | Operator Market Share & Gross Revenue | Stacked Bar / Table | `total_trips`, `gross_revenue`, `tip_rate_pct` |
| **3.2** | Payment Method Adoption | 100% Stacked Area Chart | Credit Card vs Cash vs Mobile Pay share over time |
| **3.3** | Driver Tipping Propensity | Histogram / Scatter | Tips as percentage of fare across payment types |
| **3.4** | Unit Economics Matrix | Scatter Plot | X: `avg_trip_distance_km`<br>Y: `fare_per_km` |

### Key Business Insights
- **Digital Payment Shift:** Credit Card and Mobile Pay account for >88% of trips and have an average tip rate of 17.8%, compared to <3% reported on Cash trips.
- **Effective Driver Yield:** Fares per km peak in downtown dense traffic ($3.80/km) due to wait-time metering, but drop to $1.95/km on freeway trips to the Peninsula.

---

## 🛠️ Step-by-Step Setup Guide

### Option A: Local Development with Metabase (Docker)

1. **Start the Metabase container:**
   ```bash
   docker compose -f docker/docker-compose.yml up -d metabase
   ```
2. **Access the Web UI:**
   Open [http://localhost:3000](http://localhost:3000) in your browser.
3. **Complete initial admin setup:**
   - Name: `Data Engineer`
   - Email: `admin@urbanpulse.local`
   - Password: `AdminPassword123!`
4. **Connect to Database:**
   - Database Type: `PostgreSQL` / `Redshift`
   - Host: `postgres` (or `localhost` if connecting from host)
   - Port: `5432`
   - Database Name: `urbanpulse_dw`
   - Username / Password: from `.env` (`postgres` / `postgres`)
5. **Import Queries:**
   - Execute the ready-to-run queries in [`docker/metabase/setup_dashboards.sql`](../docker/metabase/setup_dashboards.sql).

---

### Option B: Production Setup with Amazon QuickSight

1. **Authorize QuickSight IP in Redshift Security Group:**
   Ensure the QuickSight service role or IP range for `us-west-2` has access to port `5439` on the Redshift cluster created in Step 1.
2. **Create QuickSight Data Source:**
   - Source: Amazon Redshift (Provisioned or Serverless)
   - Cluster: `urbanpulse-redshift-dev`
   - Database: `urbanpulse_dw`
3. **Create SPICE Datasets:**
   - Select tables: `marts.mart_daily_ops`, `marts.mart_zone_perf`, `marts.mart_revenue`.
   - Set refresh schedule: Daily incremental refresh at 04:00 UTC (post-Airflow batch DAG).
4. **Publish Dashboards:**
   - Share dashboards with appropriate IAM user groups (`FleetOps`, `ExecutiveLeadership`).
