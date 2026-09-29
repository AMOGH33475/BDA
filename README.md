# Big Data Analytics of NYC Yellow Taxi Trip Records Using Apache Spark

An end-to-end Big Data Analytics pipeline developed for the **Semester 7 Big Data Analytics (BDA)** curriculum using **Apache Spark (PySpark)**, processing **4.11 GB** of official NYC Yellow Taxi trip records in **Parquet format** (~313M+ trip events) and generating aggregated analytical outputs for **Power BI** visualization and executive presentations.

---

## 📽️ PRESENTATION SLIDE-BY-SLIDE OUTLINE (FOR REPLIT / GAMMA / PPT GENERATION)

> **Instructions for AI Slide Generators (Replit AI, Gamma.app, ChatGPT)**: 
> Use the following slide-by-slide structure to generate a 10-slide PowerPoint Presentation (.pptx).

```markdown
### SLIDE 1: Title Slide
- Title: Big Data Analytics of NYC Yellow Taxi Trip Records
- Subtitle: Scalable Data Pipeline & Insights Using Apache Spark & PySpark
- Course: Semester 7 Big Data Analytics (BDA) Project
- Key Highlights: 313.27 Million Records | $5.036 Billion Revenue | 4.11 GB Parquet Dataset

### SLIDE 2: Executive Project Summary
- Problem Statement: Processing massive scale urban transportation dataset (300M+ trips) on local/distributed infrastructure.
- Core Processing Engine: Apache Spark 4.2.0 (PySpark API) + Java OpenJDK 17.
- Dataset: Official NYC TLC Yellow Taxi Trip Records across 28 monthly Parquet files (2015–2017).
- Outcome: 5-Stage Automated Pipeline generating Clean Parquet Datasets, Power BI CSV Exports, and Visual Charts.

### SLIDE 3: End-to-End Pipeline Architecture
- Stage 1 (Ingestion): Session setup, Parquet binary reading, schema evolution handling, null profiling.
- Stage 2 (Cleaning): Single-pass domain validation (filtering invalid fares, distances, dates, passenger counts).
- Stage 3 (Transformation): Feature engineering (trip duration, speed MPH, tip %, temporal features, payment mapping).
- Stage 4 (Analytics & SQL): In-memory caching (`df.cache()`), PySpark SQL aggregations, DAG plan optimization.
- Stage 5 (Export): Generating 8 Power BI summary CSVs and 5 Seaborn/Matplotlib visualization charts.

### SLIDE 4: Apache Spark Core Concepts & Optimization
- Lazy Evaluation: DAG physical plan construction before execution; zero unnecessary RAM loading.
- Binary Parquet Format: Columnar compression, fast predicate pushdown, 4.11 GB disk storage (~20GB uncompressed RAM equivalent).
- Memory Caching & Persistence: `df.cache()` prevents redundant re-reading across 7 SQL analytics passes.
- Windowing & Spatial Joins: Joining 313M trip records with NYC Taxi Zone Lookup metadata in sub-minute execution.

### SLIDE 5: Data Hygiene & Domain Validation Rules
- Duration Filter: `dropoff_datetime > pickup_datetime` (1 to 1,440 mins) — eliminates 0-second test trips.
- Passenger Count: `1 to 9 passengers` — enforces legal NYC TLC cab capacity, removing negative meter glitches.
- Trip Distance: `0.01 to 500.0 miles` — removes 0-mile canceled trips and GPS tracking anomalies.
- Financial Filters: `fare_amount ($2.50 to $1,000)` & `total_amount ($2.50 to $1,500)` — eliminates keypunch errors.

### SLIDE 6: Macro Analytics & Financial Overview
- Total Trips Processed: 313,270,632 clean trip records.
- Total Gross Revenue: $5,036,877,204.83 ($5.036 Billion USD).
- Average Trip Metrics:
  - Average Fare: $12.90
  - Average Trip Distance: 2.98 miles
  - Average Trip Duration: 15.94 minutes
  - Average Tip Percentage: 13.72%

### SLIDE 7: Temporal & Demand Dynamics
- Rush Hour Peaks: Peak trip volume occurs at 8:00 AM (Morning Rush) and 5:00 PM – 7:00 PM (Evening Rush).
- Highest Revenue Hours: 6:00 PM ($302.9M total revenue) & 7:00 PM ($299.6M total revenue).
- Day of Week Trends: Fridays (47.8M trips) and Saturdays (48.1M trips) show peak volume; Mondays lowest (39.9M trips).
- Monthly Evolution: Volume peaked in mid-2015 (~13.2M trips/month) with steady high-revenue stability through 2017.

### SLIDE 8: Payment Behavior & Tipping Economics
- Credit Card Dominance: 203.1M trips (64.8% volume) | $3.56 Billion (70.7% revenue) | Avg Tip: $2.70 (21.15%).
- Cash Usage: 109.0M trips (34.8% volume) | $1.45 Billion (28.9% revenue) | Avg Fare: $12.02.
- Key Insight: POS in-cab digital prompt screens drive an average 21.15% tip for credit card payments; cash tips are unrecorded on meters.

### SLIDE 9: Spatial & Borough Traffic Distribution
- Borough Breakdown:
  - Manhattan: 285.3M trips (88.4% volume) | $4.09 Billion revenue | Avg Distance: 2.46 miles.
  - Queens (Airports): 17.8M trips | $770.4 Million revenue | Avg Fare: $34.73 | Avg Distance: 11.18 miles.
  - Brooklyn: 5.4M trips | $91.7 Million revenue.
- Top 3 Pickup Zones:
  1. Upper East Side South (11.71M trips, $142.3M revenue)
  2. Midtown Center (11.13M trips, $164.6M revenue)
  3. Upper East Side North (10.67M trips, $135.4M revenue)

### SLIDE 10: Deliverables & Conclusion
- Modular Architecture: 5 separate PySpark scripts + 1 Master Orchestrator (`run_pipeline.py`).
- Power BI Integration: 8 clean, lightweight CSV tables ready for Power BI desktop dashboarding (`results/csv/`).
- Visualization Outputs: 5 publication-ready PNG charts (`results/charts/`).
- Conclusion: Demonstrated enterprise Big Data processing, zero data corruption, and high-performance Spark execution.
```

---

## 📌 Executive Project Overview

* **Project Title**: Big Data Analytics of NYC Yellow Taxi Trip Records Using Apache Spark
* **Primary Engine**: Apache Spark 4.2.0 (PySpark API) running on Java OpenJDK 17
* **Raw Dataset**: Official NYC TLC Yellow Taxi Trip Records (Parquet format)
* **Dataset Location**: `NYC_Taxi_Data/`
* **Raw Data Size**: 4.11 GB across 28 monthly Parquet files (2015–2017)
* **Data Processing Rules**: Raw data is strictly read-only and never modified.
* **Output Targets**: 
  * Cleaned & Transformed Parquet Datasets (`processed_data/`)
  * Aggregated Analytical CSVs for Power BI Desktop (`results/csv/`)
  * High-Resolution Visualization Charts (`results/charts/`)
  * Detailed Audit Logs for all stages (`logs/`)

---

## 🏗️ Project Directory Architecture

```
DATASET/
├── download_taxi.py             # Script to download official TLC Parquet files
├── download_zone_lookup.py      # Script to download official Taxi Zone Lookup CSV
├── taxi_zone_lookup.csv         # Official TLC Zone & Borough metadata mapping
├── run_pipeline.py              # Master pipeline orchestrator script
│
├── NYC_Taxi_Data/               # RAW READ-ONLY PARQUET DATASET (4.11 GB, 28 files)
│   ├── yellow_tripdata_2015-01.parquet
│   ├── ...
│   └── yellow_tripdata_2017-04.parquet
│
├── src/                         # MODULAR PYSPARK PIPELINE SCRIPTS
│   ├── 01_ingestion.py          # Stage 1: PySpark session, schema inspection & null profiling
│   ├── 02_cleaning.py           # Stage 2: Domain validation & outlier filtering
│   ├── 03_transformation.py     # Stage 3: Temporal, speed & financial feature engineering
│   ├── 04_analysis.py           # Stage 4: PySpark DataFrames & Spark SQL analytics
│   └── 05_export_results.py     # Stage 5: Export CSVs for Power BI & matplotlib charts
│
├── processed_data/              # PARQUET STAGING STORAGE
│   ├── clean_taxi_data/         # Filtered clean Parquet dataset
│   └── transformed_taxi_data/   # Feature-engineered Parquet dataset
│
├── results/                     # ANALYTICAL OUTPUTS
│   ├── csv/                     # Lightweight aggregated CSVs for Power BI
│   │   ├── overall_metrics.csv
│   │   ├── hourly_analysis.csv
│   │   ├── daily_analysis.csv
│   │   ├── monthly_analysis.csv
│   │   ├── payment_analysis.csv
│   │   ├── passenger_analysis.csv
│   │   ├── top_pickup_zones.csv
│   │   └── borough_analysis.csv
│   └── charts/                  # High-resolution static chart images (.png)
│       ├── hourly_trip_distribution.png
│       ├── monthly_revenue_trend.png
│       ├── payment_type_distribution.png
│       ├── passenger_count_analysis.png
│       └── top_pickup_zones.png
│
├── logs/                        # EXECUTION AUDIT LOGS
│   ├── 01_ingestion.log
│   ├── 02_cleaning.log
│   ├── 03_transformation.log
│   ├── 04_analysis.log
│   └── 05_export_results.log
│
└── README.md                    # Project documentation & evaluation guide
```

---

## ⚡ Apache Spark Architecture & Distributed Concepts

### 1. Local Execution vs. Multi-Node Distributed Cluster Model
* **Execution Environment**: This pipeline runs Apache Spark locally using `master("local[4]")` on a workstation with multi-threaded executor parallelization.
* **How Spark Scales to Multi-Node Clusters**: The codebase utilizes PySpark's native `DataFrame` and `Spark SQL` abstractions. In a multi-node enterprise cluster (e.g., AWS EMR or Databricks), the driver node submits DAG execution plans to a cluster manager (YARN, Kubernetes, or Spark Standalone), which distributes tasks across hundreds of worker nodes without requiring any code changes.

### 2. Key Apache Spark Concepts Demonstrated
* **Lazy Evaluation**: Transformations (`.filter()`, `.withColumn()`, `.select()`) build a Directed Acyclic Graph (DAG) without loading raw data into uncompressed memory until an action (`.count()`, `.collect()`, `.write`) is invoked.
* **Schema Evolution & Parquet Reading**: Spark inspects Parquet binary footers and automatically unifies evolving schemas across different years.
* **Caching & Persistence**: Aggregated DataFrames are explicitly persisted in memory using `df.cache()`, avoiding redundant re-reading of 4.11 GB Parquet files across 7 SQL analytical passes.

---

## 🧹 Data Cleaning & Validation Rules

| Feature | Cleaning Constraint | Domain Justification |
| :--- | :--- | :--- |
| **Datetimes & Duration** | `dropoff_datetime > pickup_datetime`<br>`duration BETWEEN 1.0 AND 1440.0 min` | Ensures chronological order and removes 0-second test trips or multi-day corrupted timestamp anomalies. |
| **Passenger Count** | `passenger_count BETWEEN 1 AND 9` | NYC yellow taxicabs legally carry 1 to 9 passengers. 0 or negative values indicate unrecorded meter glitches. |
| **Trip Distance** | `trip_distance BETWEEN 0.01 AND 500.0 miles` | Eliminates 0-mile canceled trips and extreme GPS tracking errors. |
| **Fare Amount** | `fare_amount BETWEEN $2.50 AND $1000.00` | NYC TLC standard initial base charge is $2.50. Removes negative fare errors and excessive keypunch input errors. |
| **Total Amount** | `total_amount BETWEEN $2.50 AND $1500.00` | Enforces valid total trip charges including tolls, tip, and surcharges. |

---

## 🚀 Execution Instructions

```powershell
# Run Master Orchestrator
python run_pipeline.py
```