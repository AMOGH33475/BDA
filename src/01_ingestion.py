import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, isnan

# ============================================================
# PATHS AND LOGGING CONFIGURATION
# ============================================================
BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
DATASET_DIR = os.path.join(BASE_DIR, "NYC_Taxi_Data")
ZONE_LOOKUP_PATH = os.path.join(BASE_DIR, "taxi_zone_lookup.csv")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "01_ingestion.log")

# Auto-detect Java JDK if JAVA_HOME is not loaded in current terminal session
def setup_java_env():
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

logger = logging.getLogger("01_ingestion")

def get_spark_session():
    logger.info("Initializing PySpark Session for Local Cluster Execution...")
    spark_temp = r"D:\spark_temp"
    os.makedirs(spark_temp, exist_ok=True)
    return (
        SparkSession.builder
        .appName("NYC_Taxi_BigData_Ingestion")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.local.dir", spark_temp)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("       STAGE 1: SPARK INGESTION & DATA PROFILING")
    logger.info("=" * 70)
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 1. Dataset Directory Info
    parquet_files = [f for f in os.listdir(DATASET_DIR) if f.endswith(".parquet")]
    total_size_bytes = sum(os.path.getsize(os.path.join(DATASET_DIR, f)) for f in parquet_files)
    total_size_gb = total_size_bytes / (1024**3)
    
    logger.info(f"Target Directory: {DATASET_DIR}")
    logger.info(f"Parquet Files Count: {len(parquet_files)}")
    logger.info(f"Total Dataset Size on Disk: {total_size_gb:.2f} GB")
    
    # 2. Spark Parquet Ingestion
    logger.info("Ingesting Parquet dataset into PySpark DataFrame...")
    parquet_paths = [os.path.join(DATASET_DIR, f) for f in os.listdir(DATASET_DIR) if f.endswith(".parquet")]
    raw_df = spark.read.parquet(*parquet_paths)
    
    partition_count = raw_df.rdd.getNumPartitions()
    logger.info(f"PySpark DataFrame Partitions: {partition_count}")
    
    # 3. Total Record Count
    logger.info("Calculating total record count across all Parquet files...")
    total_records = raw_df.count()
    logger.info(f"TOTAL RAW RECORDS INGESTED: {total_records:,}")
    
    # 4. Schema & Data Types Inspection
    logger.info("Raw Dataset Schema & Data Types:")
    schema_str = raw_df._jdf.schema().treeString()
    logger.info("\n" + schema_str)
    
    # 5. Missing / Null Value Profiling
    logger.info("Profiling Missing / Null / NaN Values per Column...")
    null_counts_expr = [
        count(when(isnull(c) | isnan(c), c)).alias(c) 
        if dtype in ["double", "float"] 
        else count(when(isnull(c), c)).alias(c)
        for c, dtype in raw_df.dtypes
    ]
    null_counts_df = raw_df.select(null_counts_expr).collect()[0].asDict()
    
    logger.info(f"{'Column Name':<30} | {'Null Count':<12} | {'Null Percentage':<15}")
    logger.info("-" * 65)
    for col_name, null_count in null_counts_df.items():
        pct = (null_count / total_records) * 100
        logger.info(f"{col_name:<30} | {null_count:<12,} | {pct:<14.2f}%")
        
    # 6. Duplicate Record Check (Key-based representative profiling)
    logger.info("Checking for duplicate records (Representative Key Sample)...")
    sample_df = raw_df.sample(withReplacement=False, fraction=0.01, seed=42)
    sample_total = sample_df.count()
    key_cols = [c for c in ["tpep_pickup_datetime", "pickup_datetime", "passenger_count", "trip_distance", "fare_amount"] if c in raw_df.columns]
    sample_distinct = sample_df.dropDuplicates(key_cols).count()
    dup_pct = ((sample_total - sample_distinct) / sample_total) * 100
    logger.info(f"Duplicate Record Profile: estimated {dup_pct:.2f}% duplicate rate across raw dataset.")
    
    # 7. Ingest Taxi Zone Lookup CSV
    logger.info("Ingesting Taxi Zone Lookup CSV...")
    if os.path.exists(ZONE_LOOKUP_PATH):
        zone_df = spark.read.csv(ZONE_LOOKUP_PATH, header=True, inferSchema=True)
        logger.info(f"Taxi Zone Lookup Records: {zone_df.count()}")
        logger.info("Taxi Zone Schema:")
        zone_df.printSchema()
    else:
        logger.warning(f"Taxi zone lookup file not found at {ZONE_LOOKUP_PATH}")
        
    # 8. Sample Records Output
    logger.info("Displaying 5 sample raw records:")
    sample_rows = raw_df.limit(5).toPandas()
    logger.info("\n" + sample_rows.to_string())
    
    elapsed_time = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"STAGE 1 COMPLETE: Ingestion & Profiling finished in {elapsed_time:.2f} seconds.")
    logger.info("=" * 70)
    
    spark.stop()

if __name__ == "__main__":
    main()
