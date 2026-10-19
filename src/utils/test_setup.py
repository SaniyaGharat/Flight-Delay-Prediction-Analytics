"""
Smoke-test script for the Flight Delay Analytics project setup.
Strict HDFS-only verification:
1. Validates HDFS NameNode availability at localhost:9000 (fails loudly if offline).
2. Initializes SparkSession configured with fs.defaultFS = hdfs://localhost:9000.
3. Writes a sample flight delay DataFrame to hdfs:///user/$USER/flight_delay/tmp_smoke_test as Parquet.
4. Confirms the written path appears in HDFS via `hdfs dfs -ls`.
5. Reads the Parquet dataset back from HDFS and validates row count and schema.
6. Cleans up the temporary HDFS directory using the Hadoop FileSystem API and confirms deletion.
"""

import os
import sys
import getpass
import subprocess
from pathlib import Path
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Ensure project root is in sys.path
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, is_hdfs_available


def run_smoke_test() -> bool:
    print("=" * 75)
    print(" FLIGHT DELAY ANALYTICS - HDFS SMOKE TEST (PHASE 0)")
    print("=" * 75)

    # Resolve active Linux user
    username = os.environ.get("USER") or getpass.getuser()
    hdfs_base_uri = "hdfs://localhost:9000"
    smoke_rel_path = f"/user/{username}/flight_delay/tmp_smoke_test"
    target_hdfs_path = f"{hdfs_base_uri}{smoke_rel_path}"

    print(f"[PRE-CHECK] Checking HDFS NameNode reachability at localhost:9000...")
    if not is_hdfs_available("localhost", 9000):
        print("\n" + "!" * 75, file=sys.stderr)
        print(" [FATAL ERROR] HDFS NameNode is OFFLINE at localhost:9000!", file=sys.stderr)
        print(" Local file fallback is DISABLED. HDFS must be running for Phase 0.", file=sys.stderr)
        print(" Start HDFS with: ~/hadoop/sbin/start-dfs.sh", file=sys.stderr)
        print("!" * 75, file=sys.stderr)
        raise RuntimeError("HDFS NameNode is offline at localhost:9000. Smoke test aborted.")

    print(f"[PRE-CHECK] HDFS NameNode is ONLINE. Target path: {target_hdfs_path}")

    # Step 1: Initialize SparkSession with HDFS defaultFS
    print(f"\n[STEP 1] Initializing PySpark Session with HDFS integration...")
    spark = get_spark_session("FlightDelay-HDFS-SmokeTest", enable_hdfs=True)
    sc = spark.sparkContext
    configured_fs = spark.conf.get("spark.hadoop.fs.defaultFS")
    print(f" -> Spark version: {spark.version}")
    print(f" -> Spark master: {sc.master}")
    print(f" -> Python interpreter: {sys.executable} (Python {sys.version.split()[0]})")
    print(f" -> Configured defaultFS: {configured_fs}")

    if configured_fs != hdfs_base_uri:
        raise ValueError(f"Expected defaultFS '{hdfs_base_uri}', but got '{configured_fs}'")

    # Step 2: Create sample flight delay dataset
    print(f"\n[STEP 2] Creating sample Flight Delay DataFrame...")
    schema = StructType([
        StructField("Carrier", StringType(), False),
        StructField("FlightNum", IntegerType(), False),
        StructField("Origin", StringType(), False),
        StructField("Dest", StringType(), False),
        StructField("DepDelayMinutes", DoubleType(), True),
        StructField("ArrDelayMinutes", DoubleType(), True),
        StructField("IsDelayed", IntegerType(), False),
    ])

    test_data = [
        ("AA", 101, "JFK", "LAX", 15.0, 20.0, 1),
        ("DL", 204, "ATL", "SFO", -2.0, -5.0, 0),
        ("UA", 305, "ORD", "MIA", 45.0, 50.0, 1),
        ("WN", 412, "DEN", "LAS", 0.0, 2.0, 0),
        ("B6", 523, "BOS", "MCO", 60.0, 72.0, 1),
    ]

    df_original = spark.createDataFrame(test_data, schema=schema)
    expected_rows = len(test_data)
    print(f" -> Generated {expected_rows} sample records across {len(schema.fields)} schema fields.")

    # Step 3: Write DataFrame directly to HDFS
    print(f"\n[STEP 3] Writing DataFrame as Parquet directly to HDFS: {target_hdfs_path}...")
    df_original.write.mode("overwrite").parquet(target_hdfs_path)
    print(" -> HDFS Parquet write completed successfully.")

    # Step 4: Verify path in HDFS via 'hdfs dfs -ls'
    print(f"\n[STEP 4] Verifying path in HDFS via 'hdfs dfs -ls {smoke_rel_path}'...")
    ls_result = subprocess.run(
        ["hdfs", "dfs", "-ls", smoke_rel_path],
        capture_output=True,
        text=True,
    )
    if ls_result.returncode != 0:
        raise RuntimeError(
            f"`hdfs dfs -ls {smoke_rel_path}` failed (code {ls_result.returncode}):\n{ls_result.stderr}"
        )
    print(f" -> CLI verification succeeded. HDFS directory listing:\n{ls_result.stdout.strip()}")

    # Step 5: Read DataFrame back from HDFS
    print(f"\n[STEP 5] Reading Parquet dataset back from HDFS...")
    df_read = spark.read.parquet(target_hdfs_path)
    actual_rows = df_read.count()
    print(f" -> Records retrieved from HDFS: {actual_rows}")

    print(f"\n[STEP 6] Validating Schema and Records:")
    df_read.printSchema()
    df_read.show(truncate=False)

    assert actual_rows == expected_rows, (
        f"[VALIDATION FAILED] Expected {expected_rows} rows from HDFS, retrieved {actual_rows}"
    )
    print(f"[VALIDATION] Row count assertion: PASSED ({actual_rows} == {expected_rows})")

    # Step 7: Clean up temporary HDFS test directory using Hadoop FileSystem API
    print(f"\n[STEP 7] Cleaning up temporary test path in HDFS: {smoke_rel_path}...")
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path_obj = sc._gateway.jvm.org.apache.hadoop.fs.Path(target_hdfs_path)
    fs = path_obj.getFileSystem(hadoop_conf)

    if fs.exists(path_obj):
        deleted = fs.delete(path_obj, True)
        print(f" -> Hadoop FileSystem delete status: {deleted}")
        assert not fs.exists(path_obj), "HDFS test directory still exists after delete!"
        print(" -> Verified HDFS directory successfully removed.")
    else:
        print(" -> Notice: Target path did not exist during cleanup step.")

    spark.stop()
    print("\n" + "=" * 75)
    print(" [RESULT] ALL HDFS SMOKE TEST STEPS PASSED SUCCESSFULLY")
    print("=" * 75)
    return True


if __name__ == "__main__":
    try:
        success = run_smoke_test()
        sys.exit(0 if success else 1)
    except Exception as err:
        print(f"\n[ERROR] Smoke test failed: {err}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
