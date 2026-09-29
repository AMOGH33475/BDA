import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, date_format,
    unix_timestamp, when, round as spark_round
)

# ============================================================
# PATHS AND LOGGING CONFIGURATION
# ============================================================
BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")
CLEAN_DATA_DIR = os.path.join(PROCESSED_DIR, "clean_taxi_data")
TRANSFORMED_DATA_DIR = os.path.join(PROCESSED_DIR, "transformed_taxi_data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TRANSFORMED_DATA_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "03_transformation.log")

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

logger = logging.getLogger("03_transformation")

def get_spark_session():
    spark_temp = r"D:\spark_temp"
    os.makedirs(spark_temp, exist_ok=True)
    return (
        SparkSession.builder
        .appName("NYC_Taxi_BigData_Transformation")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.local.dir", spark_temp)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
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
    logger.info("       STAGE 3: SPARK DATA TRANSFORMATION & FEATURE ENGINEERING")
    logger.info("=" * 70)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading clean Parquet dataset...")
    df = spark.read.parquet(CLEAN_DATA_DIR)
    logger.info("Dataset loaded (lazy). Applying feature engineering transformations...")

    # ----------------------------------------------------
    # DERIVED FEATURE ENGINEERING
    # (All transformations chained in one pass — no intermediate counts)
    # ----------------------------------------------------

    # 1. Temporal Features
    transformed_df = (
        df
        .withColumn("pickup_hour", hour(col("pickup_datetime")))
        .withColumn("dropoff_hour", hour(col("dropoff_datetime")))
        .withColumn("pickup_day_of_week", date_format(col("pickup_datetime"), "EEEE"))
        .withColumn("pickup_day_num", dayofweek(col("pickup_datetime")))
        .withColumn("pickup_month", month(col("pickup_datetime")))
        .withColumn("pickup_year", year(col("pickup_datetime")))
    )

    # 2. Trip Duration (Minutes)
    transformed_df = transformed_df.withColumn(
        "trip_duration_min",
        spark_round(
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60.0,
            2
        )
    )

    # 3. Trip Speed (MPH) + filter unrealistic > 100 mph
    transformed_df = transformed_df.withColumn(
        "trip_speed_mph",
        spark_round(col("trip_distance") / (col("trip_duration_min") / 60.0), 2)
    ).filter(col("trip_speed_mph") <= 100.0)

    # 4. Tip Percentage
    transformed_df = transformed_df.withColumn(
        "tip_percentage",
        spark_round(
            when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100.0).otherwise(0.0),
            2
        )
    )

    # 5. Payment Type Name Mapping
    payment_mapping = (
        when(col("payment_type") == 1, "Credit Card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No Charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided Trip")
        .otherwise("Other")
    )
    transformed_df = transformed_df.withColumn("payment_type_name", payment_mapping)

    # ----------------------------------------------------
    # LOG SCHEMA (no count() to avoid extra data scan)
    # ----------------------------------------------------
    logger.info("Transformed DataFrame Schema:")
    transformed_df.printSchema()

    # ----------------------------------------------------
    # WRITE TRANSFORMED DATASET TO PROCESSED DIRECTORY
    # ----------------------------------------------------
    logger.info(f"Writing transformed Parquet dataset to: {TRANSFORMED_DATA_DIR}")
    logger.info("This may take several minutes. Please wait...")
    transformed_df.coalesce(8).write.mode("overwrite").parquet(TRANSFORMED_DATA_DIR)
    logger.info("Transformed dataset written successfully!")

    # Verify count from written output (fast read from smaller processed files)
    logger.info("Verifying output record count from written Parquet files...")
    final_count = spark.read.parquet(TRANSFORMED_DATA_DIR).count()
    logger.info(f"Transformed record count: {final_count:,}")

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"STAGE 3 COMPLETE: Transformation finished in {elapsed:.2f} seconds.")
    logger.info("=" * 70)
    spark.stop()

if __name__ == "__main__":
    main()
