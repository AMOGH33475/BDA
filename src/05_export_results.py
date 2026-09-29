import os
import sys
import time
import logging
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ============================================================
# PATHS AND LOGGING CONFIGURATION
# ============================================================
BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")
TRANSFORMED_DATA_DIR = os.path.join(PROCESSED_DIR, "transformed_taxi_data")
ZONE_LOOKUP_PATH = os.path.join(BASE_DIR, "taxi_zone_lookup.csv")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CSV_DIR = os.path.join(RESULTS_DIR, "csv")
CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "05_export_results.log")

# Auto-detect Java JDK if JAVA_HOME is not loaded in current terminal session
def setup_java_env():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Set HADOOP_HOME for Windows (required for PySpark Parquet writes)
    hadoop_home = r"C:\hadoop"
    if os.path.exists(hadoop_home):
        os.environ["HADOOP_HOME"] = hadoop_home
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + ";" + os.environ.get("PATH", "")

    if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ.get("JAVA_HOME", "")):
        possible_paths = [
            r"C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot",
            r"C:\Program Files\Java\jdk-17",
            r"C:\Program Files\Java\jdk-11",
            r"C:\Program Files\Microsoft\jdk-11.0.32.101-hotspot"
        ]
        for p in possible_paths:
            if os.path.exists(p):
                os.environ["JAVA_HOME"] = p
                os.environ["PATH"] = os.path.join(p, "bin") + ";" + os.environ.get("PATH", "")
                break

setup_java_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("05_export_results")

# Set Seaborn / Matplotlib Style
sns.set_theme(style="whitegrid")
plt.rcParams.update({'font.sans-serif': 'DejaVu Sans', 'font.size': 11})

def get_spark_session():
    spark_temp = r"D:\spark_temp"
    os.makedirs(spark_temp, exist_ok=True)
    return (
        SparkSession.builder
        .appName("NYC_Taxi_BigData_Export")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.local.dir", spark_temp)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.io.nativeio.enabled", "false")
        .getOrCreate()
    )

def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("       STAGE 5: EXPORT RESULTS FOR POWER BI & CHARTS")
    logger.info("=" * 70)
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Reading transformed dataset into PySpark...")
    df = spark.read.parquet(TRANSFORMED_DATA_DIR)
    df.createOrReplaceTempView("taxi_trips")
    
    has_zones = False
    if os.path.exists(ZONE_LOOKUP_PATH):
        zone_df = spark.read.csv(ZONE_LOOKUP_PATH, header=True, inferSchema=True)
        zone_df.createOrReplaceTempView("taxi_zones")
        has_zones = True

    # ----------------------------------------------------
    # 1. EXPORT AGGREGATED CSVS FOR POWER BI
    # ----------------------------------------------------
    logger.info("Calculating and exporting aggregated summary CSVs...")

    # A. Overall Metrics
    overall_pdf = spark.sql("""
        SELECT 
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance_miles,
            ROUND(AVG(trip_duration_min), 2) AS avg_trip_duration_minutes,
            ROUND(AVG(tip_amount), 2) AS avg_tip_usd,
            ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
        FROM taxi_trips
    """).toPandas()
    overall_pdf.to_csv(os.path.join(CSV_DIR, "overall_metrics.csv"), index=False)
    logger.info("Exported: overall_metrics.csv")

    # B. Hourly Analysis
    hourly_pdf = spark.sql("""
        SELECT 
            pickup_hour,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
            ROUND(AVG(trip_duration_min), 2) AS avg_duration_minutes
        FROM taxi_trips
        GROUP BY pickup_hour
        ORDER BY pickup_hour ASC
    """).toPandas()
    hourly_pdf.to_csv(os.path.join(CSV_DIR, "hourly_analysis.csv"), index=False)
    logger.info("Exported: hourly_analysis.csv")

    # C. Daily Analysis
    daily_pdf = spark.sql("""
        SELECT 
            pickup_day_num,
            pickup_day_of_week,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM taxi_trips
        GROUP BY pickup_day_num, pickup_day_of_week
        ORDER BY pickup_day_num ASC
    """).toPandas()
    daily_pdf.to_csv(os.path.join(CSV_DIR, "daily_analysis.csv"), index=False)
    logger.info("Exported: daily_analysis.csv")

    # D. Monthly Analysis
    monthly_pdf = spark.sql("""
        SELECT 
            pickup_year,
            pickup_month,
            CONCAT(CAST(pickup_year AS STRING), '-', LPAD(CAST(pickup_month AS STRING), 2, '0')) AS year_month,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM taxi_trips
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year ASC, pickup_month ASC
    """).toPandas()
    monthly_pdf.to_csv(os.path.join(CSV_DIR, "monthly_analysis.csv"), index=False)
    logger.info("Exported: monthly_analysis.csv")

    # E. Payment Analysis
    payment_pdf = spark.sql("""
        SELECT 
            payment_type_name,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(tip_amount), 2) AS avg_tip_usd,
            ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
        FROM taxi_trips
        GROUP BY payment_type_name
        ORDER BY trip_count DESC
    """).toPandas()
    payment_pdf.to_csv(os.path.join(CSV_DIR, "payment_analysis.csv"), index=False)
    logger.info("Exported: payment_analysis.csv")

    # F. Passenger Analysis
    passenger_pdf = spark.sql("""
        SELECT 
            passenger_count,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM taxi_trips
        GROUP BY passenger_count
        ORDER BY passenger_count ASC
    """).toPandas()
    passenger_pdf.to_csv(os.path.join(CSV_DIR, "passenger_analysis.csv"), index=False)
    logger.info("Exported: passenger_analysis.csv")

    # G. Spatial / Location Analysis
    if has_zones and "PULocationID" in df.columns:
        top_pu_pdf = spark.sql("""
            SELECT 
                t.PULocationID AS location_id,
                z.Borough AS borough,
                z.Zone AS zone_name,
                COUNT(*) AS pickup_trip_count,
                ROUND(SUM(t.total_amount), 2) AS total_revenue_usd,
                ROUND(AVG(t.fare_amount), 2) AS avg_fare_usd,
                ROUND(AVG(t.trip_distance), 2) AS avg_distance_miles
            FROM taxi_trips t
            JOIN taxi_zones z ON t.PULocationID = z.LocationID
            GROUP BY t.PULocationID, z.Borough, z.Zone
            ORDER BY pickup_trip_count DESC
            LIMIT 20
        """).toPandas()
        top_pu_pdf.to_csv(os.path.join(CSV_DIR, "top_pickup_zones.csv"), index=False)
        logger.info("Exported: top_pickup_zones.csv")

        borough_pdf = spark.sql("""
            SELECT 
                z.Borough AS borough,
                COUNT(*) AS total_trips,
                ROUND(SUM(t.total_amount), 2) AS total_revenue_usd,
                ROUND(AVG(t.fare_amount), 2) AS avg_fare_usd,
                ROUND(AVG(t.trip_distance), 2) AS avg_distance_miles
            FROM taxi_trips t
            JOIN taxi_zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Borough
            ORDER BY total_trips DESC
        """).toPandas()
        borough_pdf.to_csv(os.path.join(CSV_DIR, "borough_analysis.csv"), index=False)
        logger.info("Exported: borough_analysis.csv")

    # ----------------------------------------------------
    # 2. GENERATE VISUALIZATION CHARTS
    # ----------------------------------------------------
    logger.info("Generating high-resolution visualization charts...")

    # Chart 1: Hourly Trip Distribution & Revenue
    fig, ax1 = plt.subplots(figsize=(10, 5))
    color = '#1f77b4'
    ax1.set_xlabel('Hour of Day (0 - 23)')
    ax1.set_ylabel('Total Trips', color=color)
    ax1.bar(hourly_pdf['pickup_hour'], hourly_pdf['trip_count'], color=color, alpha=0.7)
    ax1.tick_params(axis='y', labelcolor=color)

    ax2 = ax1.twinx()
    color = '#ff7f0e'
    ax2.set_ylabel('Total Revenue ($)', color=color)
    ax2.plot(hourly_pdf['pickup_hour'], hourly_pdf['total_revenue_usd'], color=color, linewidth=2.5, marker='o')
    ax2.tick_params(axis='y', labelcolor=color)

    plt.title('NYC Yellow Taxi: Hourly Trip Volume & Total Revenue', fontsize=14, fontweight='bold', pad=15)
    fig.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "hourly_trip_distribution.png"), dpi=300)
    plt.close()
    logger.info("Chart created: hourly_trip_distribution.png")

    # Chart 2: Monthly Revenue Trend
    plt.figure(figsize=(11, 5))
    plt.plot(monthly_pdf['year_month'], monthly_pdf['total_revenue_usd'] / 1e6, marker='s', color='#2ca02c', linewidth=2.5)
    plt.title('NYC Yellow Taxi: Monthly Total Revenue Trend ($ Millions)', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Year-Month')
    plt.ylabel('Revenue ($ Millions)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "monthly_revenue_trend.png"), dpi=300)
    plt.close()
    logger.info("Chart created: monthly_revenue_trend.png")

    # Chart 3: Payment Type Breakdown
    plt.figure(figsize=(8, 5))
    sns.barplot(data=payment_pdf, x='payment_type_name', y='trip_count', palette='Blues_r')
    plt.title('NYC Yellow Taxi: Trips by Payment Type', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Payment Method')
    plt.ylabel('Total Trips')
    plt.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "payment_type_distribution.png"), dpi=300)
    plt.close()
    logger.info("Chart created: payment_type_distribution.png")

    # Chart 4: Passenger Count Analysis
    plt.figure(figsize=(8, 5))
    sns.barplot(data=passenger_pdf, x='passenger_count', y='trip_count', palette='Purples_r')
    plt.title('NYC Yellow Taxi: Trip Count by Passenger Capacity', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Passenger Count')
    plt.ylabel('Total Trips')
    plt.tight_layout()
    plt.savefig(os.path.join(CHARTS_DIR, "passenger_count_analysis.png"), dpi=300)
    plt.close()
    logger.info("Chart created: passenger_count_analysis.png")

    # Chart 5: Top Pickup Zones (if available)
    if has_zones and "PULocationID" in df.columns:
        plt.figure(figsize=(10, 6))
        sns.barplot(data=top_pu_pdf.head(10), y='zone_name', x='pickup_trip_count', palette='viridis')
        plt.title('Top 10 Most Popular NYC Taxi Pickup Zones', fontsize=14, fontweight='bold', pad=15)
        plt.xlabel('Total Pickups')
        plt.ylabel('Taxi Zone Name')
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "top_pickup_zones.png"), dpi=300)
        plt.close()
        logger.info("Chart created: top_pickup_zones.png")

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"STAGE 5 COMPLETE: CSVs & Charts exported in {elapsed:.2f} seconds.")
    logger.info("=" * 70)

    spark.stop()

if __name__ == "__main__":
    main()
