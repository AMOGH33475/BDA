import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg as spark_avg, round as spark_round, desc
)

# ============================================================
# PATHS AND LOGGING CONFIGURATION
# ============================================================
BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")
TRANSFORMED_DATA_DIR = os.path.join(PROCESSED_DIR, "transformed_taxi_data")
ZONE_LOOKUP_PATH = os.path.join(BASE_DIR, "taxi_zone_lookup.csv")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "04_analysis.log")

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

logger = logging.getLogger("04_analysis")

def get_spark_session():
    spark_temp = r"D:\spark_temp"
    os.makedirs(spark_temp, exist_ok=True)
    return (
        SparkSession.builder
        .appName("NYC_Taxi_BigData_Analytics")
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
    logger.info("       STAGE 4: PYSPARK BIG DATA ANALYTICS & SPARK SQL")
    logger.info("=" * 70)
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # ----------------------------------------------------
    # DEMONSTRATE SPARK LAZY EVALUATION & CACHING
    # ----------------------------------------------------
    logger.info("Demonstrating Spark Concept: Reading dataset (LAZY EVALUATION)...")
    df = spark.read.parquet(TRANSFORMED_DATA_DIR)
    
    logger.info("Demonstrating Spark Concept: Caching transformed DataFrame into Memory...")
    df.cache()
    
    # Trigger Spark Action to force cache execution
    record_count = df.count()
    logger.info(f"DataFrame Cached successfully! Total active records: {record_count:,}")
    logger.info(f"DataFrame Partitions: {df.rdd.getNumPartitions()}")
    
    # ----------------------------------------------------
    # REGISTER SPARK SQL TEMPORARY VIEWS
    # ----------------------------------------------------
    logger.info("Registering PySpark SQL Temporary Views ('taxi_trips' & 'taxi_zones')...")
    df.createOrReplaceTempView("taxi_trips")
    
    if os.path.exists(ZONE_LOOKUP_PATH):
        zone_df = spark.read.csv(ZONE_LOOKUP_PATH, header=True, inferSchema=True)
        zone_df.createOrReplaceTempView("taxi_zones")
        has_zones = True
    else:
        has_zones = False
        logger.warning("Zone lookup file not found, skipping zone name joins.")
        
    # ============================================================
    # ANALYTICS QUERY A: OVERALL METRICS
    # ============================================================
    logger.info("\n--- ANALYTICS A: OVERALL SUMMARY METRICS ---")
    overall_sql = """
        SELECT 
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance_miles,
            ROUND(AVG(trip_duration_min), 2) AS avg_trip_duration_minutes,
            ROUND(AVG(tip_amount), 2) AS avg_tip_usd,
            ROUND(AVG(tip_percentage), 2) AS avg_tip_percentage
        FROM taxi_trips
    """
    overall_df = spark.sql(overall_sql)
    logger.info("\n" + overall_df.toPandas().to_string())
    
    # ============================================================
    # ANALYTICS QUERY B: TEMPORAL & HOURLY ANALYSIS
    # ============================================================
    logger.info("\n--- ANALYTICS B: HOURLY & PEAK TRAVEL ANALYSIS ---")
    hourly_sql = """
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
    """
    hourly_df = spark.sql(hourly_sql)
    logger.info("\n" + hourly_df.toPandas().to_string())
    
    logger.info("\n--- ANALYTICS B2: DAY OF WEEK ANALYSIS ---")
    daily_sql = """
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
    """
    daily_df = spark.sql(daily_sql)
    logger.info("\n" + daily_df.toPandas().to_string())
    
    logger.info("\n--- ANALYTICS B3: MONTHLY ANALYSIS ---")
    monthly_sql = """
        SELECT 
            pickup_year,
            pickup_month,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM taxi_trips
        GROUP BY pickup_year, pickup_month
        ORDER BY pickup_year ASC, pickup_month ASC
    """
    monthly_df = spark.sql(monthly_sql)
    logger.info("\n" + monthly_df.toPandas().to_string())
    
    # ============================================================
    # ANALYTICS QUERY C & D: PAYMENT TYPE ANALYSIS
    # ============================================================
    logger.info("\n--- ANALYTICS C & D: PAYMENT TYPE & REVENUE BREAKDOWN ---")
    payment_sql = """
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
    """
    payment_df = spark.sql(payment_sql)
    logger.info("\n" + payment_df.toPandas().to_string())
    
    # ============================================================
    # ANALYTICS QUERY E: PASSENGER COUNT ANALYSIS
    # ============================================================
    logger.info("\n--- ANALYTICS E: PASSENGER COUNT ANALYSIS ---")
    passenger_sql = """
        SELECT 
            passenger_count,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(fare_amount), 2) AS avg_fare_usd,
            ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM taxi_trips
        GROUP BY passenger_count
        ORDER BY passenger_count ASC
    """
    passenger_df = spark.sql(passenger_sql)
    logger.info("\n" + passenger_df.toPandas().to_string())
    
    # ============================================================
    # ANALYTICS QUERY F: SPATIAL & TAXI ZONE LOCATION ANALYSIS
    # ============================================================
    if has_zones and "PULocationID" in df.columns:
        logger.info("\n--- ANALYTICS F: TOP PICKUP TAXI ZONES ---")
        top_pu_sql = """
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
            LIMIT 15
        """
        top_pu_df = spark.sql(top_pu_sql)
        logger.info("\n" + top_pu_df.toPandas().to_string())
        
        logger.info("\n--- ANALYTICS F2: BOROUGH LEVEL REVENUE & TRIP SUMMARY ---")
        borough_sql = """
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
        """
        borough_df = spark.sql(borough_sql)
        logger.info("\n" + borough_df.toPandas().to_string())
        
    # ----------------------------------------------------
    # DEMONSTRATE SPARK EXPLAIN EXECUTION PLAN
    # ----------------------------------------------------
    logger.info("\nDemonstrating Spark Concept: Query Execution Plan (explain()):")
    logger.info("Hourly Aggregation Logical & Physical DAG Plan:")
    hourly_df.explain(True)
    
    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"STAGE 4 COMPLETE: Analytics & SQL Queries executed in {elapsed:.2f} seconds.")
    logger.info("=" * 70)
    
    spark.stop()

if __name__ == "__main__":
    main()
