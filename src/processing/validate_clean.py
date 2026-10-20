#!/usr/bin/env python3
"""
src/processing/validate_clean.py - Validation script for Phase 3 Cleaned Datasets.

Reads back processed datasets from HDFS:
- processed/flights_clean
- processed/airports_clean
- processed/airlines_clean
- processed/airport_id_map

Runs strict assertions and prints PASS/FAIL per check:
1. Row count == 5,819,079 minus exact duplicates dropped; per-month counts match Phase 2 profile.
2. IS_FLOWN=1 count == 5,714,008; CANCELLED=1 == 89,884; DIVERTED=1 == 15,187; HAS_CAUSE_DATA=1 == 1,063,439.
3. Null counts of each casted column equal raw null counts from Phase 2.
4. No value equal to "2400" remains in the six time columns.
5. Rows having non-IATA ORIGIN/DESTINATION equal unmapped rows.
6. All non-null ORIGIN/DESTINATION IATA values exist in airports_clean.
7. FLIGHT_DATE non-null for all rows and DAY_OF_WEEK agrees with it.
8. Displays schema, 5 sample rows, and 5 October sample rows showing RAW vs mapped codes.
"""

import os
import sys
import time
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType


def run_validation():
    print("============================================================")
    print("Starting Phase 3 Cleaned Data Validation Pipeline (B5)...")
    print("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)

    flights_path = f"{namenode_uri}{processed_dir}/flights_clean"
    airports_path = f"{namenode_uri}{processed_dir}/airports_clean"
    airlines_path = f"{namenode_uri}{processed_dir}/airlines_clean"
    map_path = f"{namenode_uri}{processed_dir}/airport_id_map"

    extra_spark_cfg = {
        "spark.driver.memory": "3g",
        "spark.sql.shuffle.partitions": "24",
    }
    spark = get_spark_session(
        app_name="FlightDelay-Phase3-Validation",
        enable_hdfs=True,
        extra_configs=extra_spark_cfg,
    )

    all_passed = True

    try:
        print(f"Loading cleaned flights from {flights_path}...")
        df_flights = spark.read.parquet(flights_path)
        df_airports = spark.read.parquet(airports_path)
        df_airlines = spark.read.parquet(airlines_path)
        df_map = spark.read.parquet(map_path)

        # ----------------------------------------------------------------------
        # Check 1: Total Row Count and Monthly Distribution
        # ----------------------------------------------------------------------
        print("\n--- Check 1: Row count & monthly counts ---")
        total_rows = df_flights.count()
        expected_total = 5819079  # minus 0 exact dupes
        c1_total_pass = (total_rows == expected_total)
        print(f"Total Rows: {total_rows:,} (Expected: {expected_total:,}) -> {'PASS' if c1_total_pass else 'FAIL'}")

        phase2_monthly_expected = {
            1: 469968, 2: 429191, 3: 504312, 4: 485151,
            5: 496993, 6: 503897, 7: 520718, 8: 510536,
            9: 464946, 10: 486165, 11: 467972, 12: 479230
        }
        monthly_counts = dict(df_flights.groupBy("MONTH").count().collect())
        c1_month_pass = True
        for m in sorted(phase2_monthly_expected.keys()):
            actual_m = monthly_counts.get(m, 0)
            exp_m = phase2_monthly_expected[m]
            if actual_m != exp_m:
                c1_month_pass = False
                print(f"  Month {m}: {actual_m:,} != Expected {exp_m:,} [MISMATCH]")
            else:
                print(f"  Month {m}: {actual_m:,} == Expected {exp_m:,} [OK]")

        check1_pass = c1_total_pass and c1_month_pass
        print(f"RESULT Check 1: {'PASS' if check1_pass else 'FAIL'}")
        if not check1_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 2: Key Operational Status Flags
        # ----------------------------------------------------------------------
        print("\n--- Check 2: Operational flags (IS_FLOWN, CANCELLED, DIVERTED, HAS_CAUSE_DATA) ---")
        flag_aggs = df_flights.agg(
            F.sum("IS_FLOWN").alias("is_flown"),
            F.sum("CANCELLED").alias("cancelled"),
            F.sum("DIVERTED").alias("diverted"),
            F.sum("HAS_CAUSE_DATA").alias("has_cause_data")
        ).collect()[0]

        expected_flags = {
            "is_flown": 5714008,
            "cancelled": 89884,
            "diverted": 15187,
            "has_cause_data": 1063439
        }
        check2_pass = True
        for k, exp_val in expected_flags.items():
            act_val = int(flag_aggs[k])
            p = (act_val == exp_val)
            if not p: check2_pass = False
            print(f"  {k.upper()}: actual={act_val:,}, expected={exp_val:,} -> {'PASS' if p else 'FAIL'}")

        print(f"RESULT Check 2: {'PASS' if check2_pass else 'FAIL'}")
        if not check2_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 3: Null Counts of Casted Columns Match Phase 2
        # ----------------------------------------------------------------------
        print("\n--- Check 3: Null counts of casted columns ---")
        expected_nulls = {
            "YEAR": 0, "MONTH": 0, "DAY": 0, "DAY_OF_WEEK": 0, "FLIGHT_NUMBER": 0,
            "DISTANCE": 0, "CANCELLED": 0, "DIVERTED": 0,
            "DEPARTURE_DELAY": 86153, "ARRIVAL_DELAY": 105071,
            "TAXI_OUT": 89047, "TAXI_IN": 92513,
            "AIR_TIME": 105071, "ELAPSED_TIME": 105071, "SCHEDULED_TIME": 6,
            "AIR_SYSTEM_DELAY": 4755640, "SECURITY_DELAY": 4755640,
            "AIRLINE_DELAY": 4755640, "LATE_AIRCRAFT_DELAY": 4755640, "WEATHER_DELAY": 4755640
        }

        null_exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in expected_nulls.keys()]
        actual_nulls = df_flights.agg(*null_exprs).collect()[0].asDict()

        check3_pass = True
        for col_name, exp_cnt in expected_nulls.items():
            act_cnt = int(actual_nulls[col_name])
            p = (act_cnt == exp_cnt)
            if not p:
                check3_pass = False
                print(f"  {col_name}: actual nulls={act_cnt:,}, expected={exp_cnt:,} -> FAIL")
            else:
                print(f"  {col_name}: nulls={act_cnt:,} [MATCH]")

        print(f"RESULT Check 3: {'PASS' if check3_pass else 'FAIL'}")
        if not check3_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 4: No '2400' Values in Six Time Columns
        # ----------------------------------------------------------------------
        print("\n--- Check 4: Normalization of '2400' in six time columns ---")
        time_cols = ["SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "WHEELS_OFF", "WHEELS_ON", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME"]
        exprs_2400 = [F.sum(F.when(F.col(c) == "2400", 1).otherwise(0)).alias(c) for c in time_cols]
        counts_2400 = df_flights.agg(*exprs_2400).collect()[0].asDict()

        check4_pass = True
        for tc, c2400 in counts_2400.items():
            p = (c2400 == 0)
            if not p: check4_pass = False
            print(f"  {tc}: count of '2400' = {c2400} -> {'PASS' if p else 'FAIL'}")

        print(f"RESULT Check 4: {'PASS' if check4_pass else 'FAIL'}")
        if not check4_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 5: Non-IATA ORIGIN/DESTINATION == Unmapped Rows
        # ----------------------------------------------------------------------
        print("\n--- Check 5: Non-IATA ORIGIN/DESTINATION == unmapped rows ---")
        # In clean_flights.py, unmapped numeric airport IDs retain None in ORIGIN_AIRPORT / DESTINATION_AIRPORT
        # and are flagged with ORIGIN_UNMAPPED=1 or DEST_UNMAPPED=1
        unmapped_orig_cnt = df_flights.filter(F.col("ORIGIN_UNMAPPED") == 1).count()
        unmapped_dest_cnt = df_flights.filter(F.col("DEST_UNMAPPED") == 1).count()
        total_unmapped_rows = df_flights.filter((F.col("ORIGIN_UNMAPPED") == 1) | (F.col("DEST_UNMAPPED") == 1)).count()

        null_origin_cnt = df_flights.filter(F.col("ORIGIN_AIRPORT").isNull()).count()
        null_dest_cnt = df_flights.filter(F.col("DESTINATION_AIRPORT").isNull()).count()

        # Non-IATA remaining in ORIGIN_AIRPORT / DESTINATION_AIRPORT
        non_iata_orig = df_flights.filter(F.col("ORIGIN_AIRPORT").isNotNull() & (~F.col("ORIGIN_AIRPORT").rlike("^[A-Z]{3}$"))).count()
        non_iata_dest = df_flights.filter(F.col("DESTINATION_AIRPORT").isNotNull() & (~F.col("DESTINATION_AIRPORT").rlike("^[A-Z]{3}$"))).count()

        p_orig = (null_origin_cnt == unmapped_orig_cnt) and (non_iata_orig == 0)
        p_dest = (null_dest_cnt == unmapped_dest_cnt) and (non_iata_dest == 0)
        check5_pass = p_orig and p_dest

        print(f"  ORIGIN: unmapped flagged={unmapped_orig_cnt:,}, null IATA={null_origin_cnt:,}, non-IATA remaining={non_iata_orig} -> {'PASS' if p_orig else 'FAIL'}")
        print(f"  DESTINATION: unmapped flagged={unmapped_dest_cnt:,}, null IATA={null_dest_cnt:,}, non-IATA remaining={non_iata_dest} -> {'PASS' if p_dest else 'FAIL'}")
        print(f"  Total flight rows affected by unmapped airport IDs: {total_unmapped_rows:,}")
        print(f"RESULT Check 5: {'PASS' if check5_pass else 'FAIL'}")
        if not check5_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 6: All non-null ORIGIN/DESTINATION IATA Exist in airports_clean
        # ----------------------------------------------------------------------
        print("\n--- Check 6: All IATA codes exist in airports_clean ---")
        ref_airports = set(r["AIRPORT_CODE"] for r in df_airports.select("AIRPORT_CODE").collect())
        print(f"  Reference airport codes count: {len(ref_airports)}")

        orig_iatas = set(r["ORIGIN_AIRPORT"] for r in df_flights.filter(F.col("ORIGIN_AIRPORT").isNotNull()).select("ORIGIN_AIRPORT").distinct().collect())
        dest_iatas = set(r["DESTINATION_AIRPORT"] for r in df_flights.filter(F.col("DESTINATION_AIRPORT").isNotNull()).select("DESTINATION_AIRPORT").distinct().collect())

        missing_orig = orig_iatas - ref_airports
        missing_dest = dest_iatas - ref_airports

        check6_pass = (len(missing_orig) == 0) and (len(missing_dest) == 0)
        print(f"  Distinct flight ORIGIN IATAs: {len(orig_iatas)}, Missing from airports_clean: {len(missing_orig)} ({missing_orig})")
        print(f"  Distinct flight DEST IATAs: {len(dest_iatas)}, Missing from airports_clean: {len(missing_dest)} ({missing_dest})")
        print(f"RESULT Check 6: {'PASS' if check6_pass else 'FAIL'}")
        if not check6_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 7: FLIGHT_DATE Non-Null and DAY_OF_WEEK Agreement
        # ----------------------------------------------------------------------
        print("\n--- Check 7: FLIGHT_DATE validity and DAY_OF_WEEK agreement ---")
        null_date_cnt = df_flights.filter(F.col("FLIGHT_DATE").isNull()).count()

        # In Spark dayofweek: 1=Sun, 2=Mon, 3=Tue, 4=Wed, 5=Thu, 6=Fri, 7=Sat
        # In raw dataset: 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat, 7=Sun
        # Formula to convert Spark dayofweek to ISO/raw: ((spark_dow + 5) % 7) + 1
        mismatched_dow_cnt = df_flights.filter(
            (((F.dayofweek("FLIGHT_DATE") + 5) % 7) + 1) != F.col("DAY_OF_WEEK")
        ).count()

        check7_pass = (null_date_cnt == 0) and (mismatched_dow_cnt == 0)
        print(f"  Null FLIGHT_DATE count: {null_date_cnt} -> {'PASS' if null_date_cnt == 0 else 'FAIL'}")
        print(f"  DAY_OF_WEEK agreement mismatches: {mismatched_dow_cnt} -> {'PASS' if mismatched_dow_cnt == 0 else 'FAIL'}")
        print(f"RESULT Check 7: {'PASS' if check7_pass else 'FAIL'}")
        if not check7_pass: all_passed = False

        # ----------------------------------------------------------------------
        # Check 8: Print Schema, 5 Overall Samples, and 5 October Samples
        # ----------------------------------------------------------------------
        print("\n--- Check 8: Schema & Sample Rows ---")
        print("Schema of flights_clean:")
        df_flights.printSchema()

        print("\n5 Sample Rows (General):")
        df_flights.select(
            "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT",
            "SCHEDULED_DEPARTURE", "DEPARTURE_DELAY", "DISTANCE", "IS_FLOWN"
        ).show(5, truncate=False)

        print("\n5 Sample October Rows (Showing RAW vs Mapped Codes):")
        df_flights.filter(F.col("MONTH") == 10).select(
            "MONTH", "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER",
            "ORIGIN_AIRPORT_RAW", "ORIGIN_AIRPORT", "ORIGIN_ID_MAPPED", "ORIGIN_UNMAPPED",
            "DESTINATION_AIRPORT_RAW", "DESTINATION_AIRPORT", "DEST_ID_MAPPED", "DEST_UNMAPPED"
        ).show(5, truncate=False)

        print("============================================================")
        print(f"OVERALL VALIDATION RESULT: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
        print("============================================================")

        if not all_passed:
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_validation()
