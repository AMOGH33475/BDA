import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# ============================================================
# PATHS AND LOGGING CONFIGURATION
# ============================================================
BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
RAW_DATA_DIR = os.path.join(BASE_DIR, "NYC_Taxi_Data")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")
CLEAN_DATA_DIR = os.path.join(PROCESSED_DIR, "clean_taxi_data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "02_cleaning.log")

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

logger = logging.getLogger("02_cleaning")

def get_spark_session():
    spark_temp = r"D:\spark_temp"
    os.makedirs(spark_temp, exist_ok=True)
    return (
        SparkSession.builder
        .appName("NYC_Taxi_BigData_Cleaning")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.local.dir", spark_temp)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB per partition
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
    logger.info("       STAGE 2: SPARK DATA CLEANING & VALIDATION")
    logger.info("=" * 70)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading raw Parquet dataset...")
    parquet_paths = [
        os.path.join(RAW_DATA_DIR, f)
        for f in sorted(os.listdir(RAW_DATA_DIR))
        if f.endswith(".parquet")
    ]
    logger.info(f"Found {len(parquet_paths)} Parquet files to process.")
    df = spark.read.parquet(*parquet_paths)

    # ----------------------------------------------------
    # UNIFY COLUMN NAMES (Handle schema evolution 2015-2024)
    # ----------------------------------------------------
    col_map = {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime"
    }
    for old_col, new_col in col_map.items():
        if old_col in df.columns and new_col not in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # Calculate trip duration in minutes for validation
    df = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0
    )

    # ----------------------------------------------------
    # APPLY CLEANING & VALIDATION RULES WITH JUSTIFICATIONS
    # (All rules combined in one filter pass to minimize data scans)
    # ----------------------------------------------------
    logger.info("Applying all domain validation rules in a single filter pass...")

    # Rule 1: Valid Timestamps & Duration
    # Justification: Drop-off time must be strictly after pickup, trip duration 1-1440 min
    rule1_cond = (
        col("pickup_datetime").isNotNull() &
        col("dropoff_datetime").isNotNull() &
        (col("dropoff_datetime") > col("pickup_datetime")) &
        (col("trip_duration_minutes") >= 1.0) &
        (col("trip_duration_minutes") <= 1440.0)
    )

    # Rule 2: Passenger Count
    # Justification: NYC yellow taxis legally transport 1 to 9 passengers
    rule2_cond = (col("passenger_count") >= 1) & (col("passenger_count") <= 9)

    # Rule 3: Trip Distance
    # Justification: Distance must be > 0.0 and <= 500.0 miles
    rule3_cond = (col("trip_distance") > 0.0) & (col("trip_distance") <= 500.0)

    # Rule 4: Fare & Total Amount
    # Justification: NYC TLC base charge $2.50, capped at $1000/$1500
    rule4_cond = (
        (col("fare_amount") >= 2.50) & (col("fare_amount") <= 1000.0) &
        (col("total_amount") >= 2.50) & (col("total_amount") <= 1500.0)
    )

    # Apply all rules in a single pass
    clean_df = df.filter(rule1_cond & rule2_cond & rule3_cond & rule4_cond)

    # ----------------------------------------------------
    # WRITE CLEAN DATA TO PROCESSED DIRECTORY
    # (Write first, then log — avoids double-counting for summary)
    # ----------------------------------------------------
    logger.info(f"Writing clean dataset to Parquet format at: {CLEAN_DATA_DIR}")
    logger.info("This may take several minutes for 300M+ records. Please wait...")
    clean_df.coalesce(8).write.mode("overwrite").parquet(CLEAN_DATA_DIR)
    logger.info("Clean dataset written successfully!")

    # Count after write (reads from the smaller output, much faster)
    logger.info("Verifying output record count from written Parquet files...")
    final_count = spark.read.parquet(CLEAN_DATA_DIR).count()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("CLEANING SUMMARY:")
    logger.info(f"Final Clean Records  : {final_count:,}")
    logger.info(f"Data Retention Note  : Strict domain rules applied (see script comments)")
    logger.info("=" * 70)
    logger.info(f"STAGE 2 COMPLETE: Cleaning completed in {elapsed:.2f} seconds.")
    spark.stop()

if __name__ == "__main__":
    main()
