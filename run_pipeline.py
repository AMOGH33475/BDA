import os
import sys
import time
import subprocess

BASE_DIR = r"D:\COLLEGE\SEM 7\CNS\PROJECT\DATASET"
SRC_DIR = os.path.join(BASE_DIR, "src")
PYTHON_EXE = sys.executable

stages = [
    ("01_ingestion.py", "Stage 1: PySpark Data Ingestion & Profiling"),
    ("02_cleaning.py", "Stage 2: PySpark Data Cleaning & Validation"),
    ("03_transformation.py", "Stage 3: PySpark Feature Engineering & Transformation"),
    ("04_analysis.py", "Stage 4: PySpark Big Data Analytics & SQL Queries"),
    ("05_export_results.py", "Stage 5: Result Export for Power BI & Chart Generation")
]

def main():
    total_start = time.time()
    print("=" * 75)
    print("      NYC YELLOW TAXI BIG DATA ANALYTICS PIPELINE (APACHE SPARK)")
    print("=" * 75)
    print(f"Base Directory: {BASE_DIR}")
    print(f"Python Executable: {PYTHON_EXE}")
    print("=" * 75)
    print()

    for filename, description in stages:
        script_path = os.path.join(SRC_DIR, filename)
        print("-" * 75)
        print(f"STARTING: {description}")
        print(f"Script: {script_path}")
        print("-" * 75)
        
        stage_start = time.time()
        res = subprocess.run([PYTHON_EXE, script_path], cwd=BASE_DIR)
        
        if res.returncode != 0:
            print(f"\n[ERROR] Pipeline failed at {filename} with exit code {res.returncode}")
            sys.exit(res.returncode)
            
        stage_elapsed = time.time() - stage_start
        print(f"[SUCCESS] {description} completed in {stage_elapsed:.2f} seconds.\n")

    total_elapsed = time.time() - total_start
    print("=" * 75)
    print(f"PIPELINE COMPLETE: All 5 PySpark stages finished in {total_elapsed:.2f} seconds.")
    print("=" * 75)

if __name__ == "__main__":
    main()
