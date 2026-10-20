"""Validate enriched flights and dim_date datasets in HDFS."""

import os
import sys
import time
import json
from pathlib import Path
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.spark_session import get_spark_session, load_config


PHASE2_MONTH_COUNTS = {
    1: 469968,
    2: 429191,
    3: 504312,
    4: 485151,
    5: 496993,
    6: 503897,
    7: 520718,
    8: 510536,
    9: 464946,
    10: 486165,
    11: 467972,
    12: 479230
}

def validate_enriched():
    start_time = time.perf_counter()
    spark = get_spark_session("validate_enriched")
    
    print("=" * 80)
    print("PHASE 4: VALIDATION OF ENRICHED DATASET AND DIM_DATE")
    print("=" * 80)
    
    # Read enriched flights and dim_date
    df_enriched = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_enriched")
    df_dim_date = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/dim_date")
    
    all_passed = True
    
    def check(name, condition, details=""):
        nonlocal all_passed
        status = "PASS" if condition else "FAIL"
        if not condition:
            all_passed = False
        print(f"[{status}] {name} {details}")
        return condition

    # 1. Total row count and per-month counts
    total_count = df_enriched.count()
    check("Total row count == 5,819,079", total_count == 5819079, f"(Observed: {total_count:,})")
    
    month_counts_df = df_enriched.groupBy("MONTH").count().collect()
    month_counts = {row["MONTH"]: row["count"] for row in month_counts_df}
    month_match = True
    for m in range(1, 13):
        expected = PHASE2_MONTH_COUNTS[m]
        actual = month_counts.get(m, 0)
        if actual != expected:
            month_match = False
            print(f"  Month {m} mismatch: expected {expected:,}, got {actual:,}")
    check("Per-month counts equal Phase 2", month_match, f"(All 12 months match expected counts)")
    
    # 2. Airline name null count == 0
    airline_nulls = df_enriched.filter(F.col("AIRLINE_NAME").isNull()).count()
    check("AIRLINE_NAME null count == 0", airline_nulls == 0, f"(Observed nulls: {airline_nulls})")
    
    # 3. Origin airport name nulls == ORIGIN_UNMAPPED=1 rows; same for destination
    origin_unmapped_count = df_enriched.filter(F.col("ORIGIN_UNMAPPED") == 1).count()
    origin_nulls = df_enriched.filter(F.col("ORIGIN_AIRPORT_NAME").isNull()).count()
    check("ORIGIN_AIRPORT_NAME nulls == ORIGIN_UNMAPPED=1", origin_nulls == origin_unmapped_count,
          f"(Nulls: {origin_nulls}, Unmapped: {origin_unmapped_count})")
    
    dest_unmapped_count = df_enriched.filter(F.col("DEST_UNMAPPED") == 1).count()
    dest_nulls = df_enriched.filter(F.col("DEST_AIRPORT_NAME").isNull()).count()
    check("DEST_AIRPORT_NAME nulls == DEST_UNMAPPED=1", dest_nulls == dest_unmapped_count,
          f"(Nulls: {dest_nulls}, Unmapped: {dest_unmapped_count})")
    
    # 4. Delay flags and counts
    arr_del_15_count = df_enriched.filter(F.col("IS_ARR_DELAYED_15") == 1).count()
    check("IS_ARR_DELAYED_15=1 count == 1,063,439", arr_del_15_count == 1063439, f"(Observed: {arr_del_15_count:,})")
    
    dep_del_15_count = df_enriched.filter(F.col("IS_DEP_DELAYED_15") == 1).count()
    check("IS_DEP_DELAYED_15=1 count == 1,050,422", dep_del_15_count == 1050422, f"(Observed: {dep_del_15_count:,})")
    
    arr_del_gt0_flown = df_enriched.filter((F.col("IS_FLOWN") == 1) & (F.col("ARRIVAL_DELAY") > 0)).count()
    check("ARRIVAL_DELAY > 0 among flown == 2,086,896", arr_del_gt0_flown == 2086896, f"(Observed: {arr_del_gt0_flown:,})")
    
    # 5. Labels null for exactly non-flown rows
    non_flown_count = df_enriched.filter(F.col("IS_FLOWN") == 0).count()
    flown_count = df_enriched.filter(F.col("IS_FLOWN") == 1).count()
    check("IS_FLOWN=0 count == 105,071", non_flown_count == 105071, f"(Observed non-flown: {non_flown_count:,})")
    
    null_arr_del_15 = df_enriched.filter(F.col("IS_ARR_DELAYED_15").isNull()).count()
    null_dep_del_15 = df_enriched.filter(F.col("IS_DEP_DELAYED_15").isNull()).count()
    null_del_cat = df_enriched.filter(F.col("DELAY_CATEGORY").isNull()).count()
    
    labels_null_match = (null_arr_del_15 == 105071 and null_dep_del_15 == 105071 and null_del_cat == 105071)
    check("Labels are null for exactly the 105,071 non-flown rows", labels_null_match,
          f"(Null counts: IS_ARR_DELAYED_15={null_arr_del_15:,}, IS_DEP_DELAYED_15={null_dep_del_15:,}, DELAY_CATEGORY={null_del_cat:,})")
    
    non_null_arr_del_15 = df_enriched.filter(F.col("IS_ARR_DELAYED_15").isNotNull()).count()
    check("Non-null IS_ARR_DELAYED_15 == IS_FLOWN=1 (5,714,008)", non_null_arr_del_15 == flown_count,
          f"(Non-null IS_ARR_DELAYED_15: {non_null_arr_del_15:,}, IS_FLOWN=1: {flown_count:,}, Gap: {flown_count - non_null_arr_del_15})")
    
    # 6. STATUS counts
    status_counts_df = df_enriched.groupBy("STATUS").count().collect()
    status_counts = {row["STATUS"]: row["count"] for row in status_counts_df}
    completed_ok = (status_counts.get("Completed") == 5714008)
    cancelled_ok = (status_counts.get("Cancelled") == 89884)
    diverted_ok = (status_counts.get("Diverted") == 15187)
    check("STATUS counts match (Completed: 5,714,008, Cancelled: 89,884, Diverted: 15,187)",
          completed_ok and cancelled_ok and diverted_ok,
          f"(Observed: Completed={status_counts.get('Completed', 0):,}, Cancelled={status_counts.get('Cancelled', 0):,}, Diverted={status_counts.get('Diverted', 0):,})")
    
    # 7. SCHED_DEP_HOUR within 0-23, no null for any row
    sched_dep_hour_nulls = df_enriched.filter(F.col("SCHED_DEP_HOUR").isNull()).count()
    sched_dep_hour_range = df_enriched.filter((F.col("SCHED_DEP_HOUR") < 0) | (F.col("SCHED_DEP_HOUR") > 23)).count()
    check("SCHED_DEP_HOUR within 0-23 and no nulls", (sched_dep_hour_nulls == 0 and sched_dep_hour_range == 0),
          f"(Nulls: {sched_dep_hour_nulls}, Out of range: {sched_dep_hour_range})")
    
    # 8. PRIMARY_DELAY_CAUSE non-null count and null logic
    has_cause_1_count = df_enriched.filter(F.col("HAS_CAUSE_DATA") == 1).count()
    cause_cols = ["AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]
    all_zero_condition = " AND ".join([f"({c} == 0)" for c in cause_cols])
    causes_all_zero_count = df_enriched.filter((F.col("HAS_CAUSE_DATA") == 1) & F.expr(all_zero_condition)).count()
    primary_cause_nulls = df_enriched.filter(F.col("PRIMARY_DELAY_CAUSE").isNull()).count()
    primary_cause_non_null = df_enriched.filter(F.col("PRIMARY_DELAY_CAUSE").isNotNull()).count()
    
    expected_null_primary = (5819079 - has_cause_1_count) + causes_all_zero_count
    check("PRIMARY_DELAY_CAUSE null exactly where HAS_CAUSE_DATA=0 or all causes zero",
          primary_cause_nulls == expected_null_primary,
          f"(Observed nulls: {primary_cause_nulls:,}, Expected nulls: {expected_null_primary:,}, Non-null: {primary_cause_non_null:,}, All-zero among cause data: {causes_all_zero_count:,})")
    
    # 9. IS_WEEKEND consistency with DAY_OF_WEEK; IS_HOLIDAY_WINDOW day count equals dim_date
    weekend_inconsistency = df_enriched.filter(
        ((F.col("DAY_OF_WEEK").isin([6, 7])) & (F.col("IS_WEEKEND") != 1)) |
        ((~F.col("DAY_OF_WEEK").isin([6, 7])) & (F.col("IS_WEEKEND") != 0))
    ).count()
    check("IS_WEEKEND consistent with DAY_OF_WEEK", weekend_inconsistency == 0, f"(Inconsistencies: {weekend_inconsistency})")
    
    dim_date_holiday_days = df_dim_date.filter(F.col("IS_HOLIDAY_WINDOW") == 1).count()
    enriched_holiday_dates = df_enriched.filter(F.col("IS_HOLIDAY_WINDOW") == 1).select("FLIGHT_DATE").distinct().count()
    check("IS_HOLIDAY_WINDOW distinct date count equals dim_date's",
          dim_date_holiday_days == enriched_holiday_dates,
          f"(Enriched distinct holiday dates: {enriched_holiday_dates}, dim_date holiday rows: {dim_date_holiday_days})")
    
    print("\n" + "=" * 80)
    print("DISTRIBUTION TABLES")
    print("=" * 80)
    
    print("\n--- DISTANCE_BAND Distribution ---")
    df_enriched.groupBy("DISTANCE_BAND").count().withColumn("pct", F.round(F.col("count") / total_count * 100, 2)).orderBy(F.desc("count")).show()
    
    print("\n--- DEP_TIME_BLOCK Distribution ---")
    df_enriched.groupBy("DEP_TIME_BLOCK").count().withColumn("pct", F.round(F.col("count") / total_count * 100, 2)).orderBy(F.desc("count")).show()
    
    print("\n--- SEASON Distribution ---")
    df_enriched.groupBy("SEASON").count().withColumn("pct", F.round(F.col("count") / total_count * 100, 2)).orderBy(F.desc("count")).show()
    
    print("\n--- DELAY_CATEGORY Distribution ---")
    df_enriched.groupBy("DELAY_CATEGORY").count().withColumn("pct", F.round(F.col("count") / total_count * 100, 2)).orderBy(F.desc("count")).show()
    
    print("\n--- PRIMARY_DELAY_CAUSE Distribution ---")
    df_enriched.groupBy("PRIMARY_DELAY_CAUSE").count().withColumn("pct", F.round(F.col("count") / total_count * 100, 2)).orderBy(F.desc("count")).show()
    
    print("\n" + "=" * 80)
    print("SCHEMA")
    print("=" * 80)
    df_enriched.printSchema()
    
    print("\n" + "=" * 80)
    print("5 GENERAL SAMPLE ROWS")
    print("=" * 80)
    sample_cols = [
        "FLIGHT_DATE", "AIRLINE", "AIRLINE_NAME", "FLIGHT_NUMBER",
        "ORIGIN_AIRPORT", "ORIGIN_CITY", "DESTINATION_AIRPORT", "DEST_CITY",
        "ROUTE", "SCHED_DEP_HOUR", "DEP_TIME_BLOCK", "DAY_OF_WEEK_NAME", "SEASON",
        "STATUS", "ARRIVAL_DELAY", "DELAY_CATEGORY", "PRIMARY_DELAY_CAUSE"
    ]
    df_enriched.select(sample_cols).show(5, truncate=False)
    
    print("\n" + "=" * 80)
    print("5 OCTOBER SAMPLE ROWS (Showing mapped vs unmapped IDs)")
    print("=" * 80)
    df_enriched.filter(F.col("MONTH") == 10).select(sample_cols + ["ORIGIN_UNMAPPED", "DEST_UNMAPPED"]).show(5, truncate=False)

    elapsed = time.perf_counter() - start_time
    metrics = {
        "script": "validate_enriched.py",
        "runtime_sec": round(elapsed, 2),
        "all_passed": all_passed,
        "status": "SUCCESS" if all_passed else "FAILED"
    }
    metrics_path = Path(PROJECT_ROOT) / "logs" / "metrics" / "validate_enriched.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as mf:
        json.dump(metrics, mf, indent=2)
    print(f"[INFO] Saved metrics to {metrics_path}")
    print(f"\n[INFO] Validation completed in {elapsed:.2f} seconds.")
    print(f"OVERALL STATUS: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
    spark.stop()
    return all_passed

if __name__ == "__main__":
    success = validate_enriched()
    sys.exit(0 if success else 1)
