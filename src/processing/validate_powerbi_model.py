#!/usr/bin/env python3
"""
src/processing/validate_powerbi_model.py

Validates the PowerBI data model:
1. Main fact totals match exact baseline counts (5,819,079 / 5,714,008 / 89,884 / 15,187 / 1,063,439 / 1,050,422)
2. Cause delay minutes sum to Phase 5 total (62,651,867)
3. No orphan keys against dimensions
4. Dimension keys unique
5. CSV row counts equal HDFS table row counts
6. Recomputed airline ratios from fact_daily_airline equal kpi_by_airline
Exits 0 on PASS, non-zero on FAIL.
"""

import os
import sys
import yaml
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def load_config():
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_spark_session():
    return SparkSession.builder \
        .appName("validate_powerbi_model") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", 12) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def main():
    config = load_config()
    processed_dir = config.get("hdfs", {}).get("processed_dir", "/user/saniya/flight_delay/processed")
    if not processed_dir.startswith("hdfs://"):
        processed_dir = f"hdfs://localhost:9000{processed_dir}"
        
    powerbi_hdfs_base = f"{processed_dir}/powerbi"
    local_csv_dir = PROJECT_ROOT / config.get("local_paths", {}).get("output_powerbi", "output/powerbi")
    
    spark = get_spark_session()
    
    print("=" * 70)
    print("  PHASE 7: POWERBI DATA MODEL VALIDATION SUITE")
    print("=" * 70)
    
    failures = []
    
    # Load Dimensions
    dim_date = spark.read.parquet(f"{powerbi_hdfs_base}/dim_date")
    dim_airline = spark.read.parquet(f"{powerbi_hdfs_base}/dim_airline")
    dim_airport = spark.read.parquet(f"{powerbi_hdfs_base}/dim_airport")
    
    # -------------------------------------------------------------------------
    # 1. Dimension Key Uniqueness & Row Counts
    # -------------------------------------------------------------------------
    print("\n--- Check 1: Dimension Uniqueness and Row Counts ---")
    
    # dim_date
    date_cnt = dim_date.count()
    date_dist = dim_date.select("DATE").distinct().count()
    if date_cnt == 365 and date_dist == 365:
        print(f"  [PASS] dim_date: {date_cnt} unique dates (expected: 365)")
    else:
        failures.append(f"dim_date count/uniqueness: count={date_cnt}, unique={date_dist}")
        print(f"  [FAIL] dim_date count/uniqueness: count={date_cnt}, unique={date_dist}")
        
    # dim_airline
    airline_cnt = dim_airline.count()
    airline_dist = dim_airline.select("code").distinct().count()
    if airline_cnt == 14 and airline_dist == 14:
        print(f"  [PASS] dim_airline: {airline_cnt} unique carrier codes (expected: 14)")
    else:
        failures.append(f"dim_airline count/uniqueness: count={airline_cnt}, unique={airline_dist}")
        print(f"  [FAIL] dim_airline count/uniqueness: count={airline_cnt}, unique={airline_dist}")
        
    # dim_airport
    airport_cnt = dim_airport.count()
    airport_dist = dim_airport.select("code").distinct().count()
    if airport_cnt == 323 and airport_dist == 323:
        print(f"  [PASS] dim_airport: {airport_cnt} unique airport codes (322 IATA + UNMAPPED)")
    else:
        failures.append(f"dim_airport count/uniqueness: count={airport_cnt}, unique={airport_dist}")
        print(f"  [FAIL] dim_airport count/uniqueness: count={airport_cnt}, unique={airport_dist}")
        
    # Check UNMAPPED row exists in dim_airport
    unmapped_cnt = dim_airport.filter(F.col("code") == "UNMAPPED").count()
    if unmapped_cnt == 1:
        print(f"  [PASS] dim_airport contains 'UNMAPPED' airport entity")
    else:
        failures.append("dim_airport missing 'UNMAPPED' entry")
        print(f"  [FAIL] dim_airport missing 'UNMAPPED' entry")
        
    # -------------------------------------------------------------------------
    # 2. Main Fact Totals Assertions
    # -------------------------------------------------------------------------
    print("\n--- Check 2: Main Fact Totals Assertions ---")
    EXPECTED_TOTALS = {
        "total_flights": 5819079,
        "flown_flights": 5714008,
        "cancelled_flights": 89884,
        "diverted_flights": 15187,
        "delayed_arr_15": 1063439,
        "delayed_dep_15": 1050422,
    }
    
    main_facts = [
        "fact_daily_airline",
        "fact_daily_origin",
        "fact_daily_dest",
        "fact_airline_dow_hour",
        "fact_route_month"
    ]
    
    for fact_name in main_facts:
        df_fact = spark.read.parquet(f"{powerbi_hdfs_base}/{fact_name}")
        sums = df_fact.select(
            F.sum("total_flights").alias("total_flights"),
            F.sum("flown_flights").alias("flown_flights"),
            F.sum("cancelled_flights").alias("cancelled_flights"),
            F.sum("diverted_flights").alias("diverted_flights"),
            F.sum("delayed_arr_15").alias("delayed_arr_15"),
            F.sum("delayed_dep_15").alias("delayed_dep_15")
        ).collect()[0]
        
        all_match = True
        fact_errors = []
        for metric, exp_val in EXPECTED_TOTALS.items():
            act_val = int(sums[metric])
            if act_val != exp_val:
                all_match = False
                fact_errors.append(f"{metric}: expected {exp_val}, got {act_val}")
                
        if all_match:
            print(f"  [PASS] {fact_name:<24}: All 6 totals match baseline exactly")
        else:
            failures.append(f"{fact_name} mismatch: {'; '.join(fact_errors)}")
            print(f"  [FAIL] {fact_name:<24}: {'; '.join(fact_errors)}")
            
    # -------------------------------------------------------------------------
    # 3. Cause Minutes and Cancellation Facts
    # -------------------------------------------------------------------------
    print("\n--- Check 3: Cause Minutes and Cancellation Facts ---")
    
    # fact_cause_daily_airline
    df_cause = spark.read.parquet(f"{powerbi_hdfs_base}/fact_cause_daily_airline")
    total_cause_min = int(round(df_cause.select(F.sum("MINUTES")).collect()[0][0]))
    if total_cause_min == 62651867:
        print(f"  [PASS] fact_cause_daily_airline: Sum of delay minutes = {total_cause_min:,} (matches Phase 5 total)")
    else:
        failures.append(f"Cause minutes mismatch: expected 62,651,867, got {total_cause_min:,}")
        print(f"  [FAIL] fact_cause_daily_airline: Sum of delay minutes = {total_cause_min:,} (expected: 62,651,867)")
        
    # fact_cancel_daily_airline_reason
    df_cancel = spark.read.parquet(f"{powerbi_hdfs_base}/fact_cancel_daily_airline_reason")
    total_cancel = int(df_cancel.select(F.sum("CANCELLED_FLIGHTS")).collect()[0][0])
    if total_cancel == 89884:
        print(f"  [PASS] fact_cancel_daily_airline_reason: Sum of cancelled flights = {total_cancel:,} (expected: 89,884)")
    else:
        failures.append(f"Cancelled flights mismatch: expected 89,884, got {total_cancel:,}")
        print(f"  [FAIL] fact_cancel_daily_airline_reason: Sum of cancelled flights = {total_cancel:,} (expected: 89,884)")
        
    # -------------------------------------------------------------------------
    # 4. Orphan Key Integrity Check
    # -------------------------------------------------------------------------
    print("\n--- Check 4: Foreign Key Integrity (No Orphan Keys) ---")
    
    # Dates
    valid_dates = set(r["DATE"] for r in dim_date.select("DATE").collect())
    # Airlines
    valid_airlines = set(r["code"] for r in dim_airline.select("code").collect())
    # Airports
    valid_airports = set(r["code"] for r in dim_airport.select("code").collect())
    
    def check_orphan(df, col_name, valid_set, table_label):
        keys = set(r[col_name] for r in df.select(col_name).distinct().collect())
        orphans = keys - valid_set
        if len(orphans) == 0:
            print(f"  [PASS] {table_label}.{col_name}: 0 orphan keys ({len(keys)} distinct keys valid)")
            return True
        else:
            failures.append(f"{table_label}.{col_name} has {len(orphans)} orphans: {list(orphans)[:5]}")
            print(f"  [FAIL] {table_label}.{col_name} has {len(orphans)} orphans: {list(orphans)[:5]}")
            return False
            
    df_fda = spark.read.parquet(f"{powerbi_hdfs_base}/fact_daily_airline")
    check_orphan(df_fda, "FLIGHT_DATE", valid_dates, "fact_daily_airline")
    check_orphan(df_fda, "AIRLINE", valid_airlines, "fact_daily_airline")
    
    df_fdo = spark.read.parquet(f"{powerbi_hdfs_base}/fact_daily_origin")
    check_orphan(df_fdo, "FLIGHT_DATE", valid_dates, "fact_daily_origin")
    check_orphan(df_fdo, "ORIGIN_AIRPORT", valid_airports, "fact_daily_origin")
    
    df_fdd = spark.read.parquet(f"{powerbi_hdfs_base}/fact_daily_dest")
    check_orphan(df_fdd, "FLIGHT_DATE", valid_dates, "fact_daily_dest")
    check_orphan(df_fdd, "DEST_AIRPORT", valid_airports, "fact_daily_dest")
    
    df_frm = spark.read.parquet(f"{powerbi_hdfs_base}/fact_route_month")
    check_orphan(df_frm, "ORIGIN", valid_airports, "fact_route_month")
    check_orphan(df_frm, "DEST", valid_airports, "fact_route_month")
    
    df_fc = spark.read.parquet(f"{powerbi_hdfs_base}/fact_cause_daily_airline")
    check_orphan(df_fc, "FLIGHT_DATE", valid_dates, "fact_cause_daily_airline")
    check_orphan(df_fc, "AIRLINE", valid_airlines, "fact_cause_daily_airline")
    
    # -------------------------------------------------------------------------
    # 5. CSV Row Counts Equal HDFS Parquet Counts
    # -------------------------------------------------------------------------
    print("\n--- Check 5: CSV Row Counts vs HDFS Parquet Counts ---")
    all_tables = [
        "dim_date", "dim_airline", "dim_airport",
        "fact_daily_airline", "fact_daily_origin", "fact_daily_dest",
        "fact_airline_dow_hour", "fact_route_month",
        "fact_cause_daily_airline", "fact_cancel_daily_airline_reason"
    ]
    
    for tbl in all_tables:
        hdfs_cnt = spark.read.parquet(f"{powerbi_hdfs_base}/{tbl}").count()
        csv_file = local_csv_dir / f"{tbl}.csv"
        if not csv_file.exists():
            failures.append(f"Missing CSV file: {csv_file}")
            print(f"  [FAIL] {tbl}.csv: File not found")
            continue
            
        with open(csv_file, "r", encoding="utf-8") as f:
            csv_lines = sum(1 for _ in f) - 1 # Subtract header
            
        if hdfs_cnt == csv_lines:
            print(f"  [PASS] {tbl:<32}: CSV rows = HDFS rows = {hdfs_cnt:,}")
        else:
            failures.append(f"{tbl} count mismatch: HDFS={hdfs_cnt}, CSV={csv_lines}")
            print(f"  [FAIL] {tbl:<32}: HDFS={hdfs_cnt:,} vs CSV={csv_lines:,}")
            
    # -------------------------------------------------------------------------
    # 6. Recomputed Ratios from fact_daily_airline vs kpi_by_airline
    # -------------------------------------------------------------------------
    print("\n--- Check 6: Recomputed Ratios from fact_daily_airline vs kpi_by_airline ---")
    df_kpi = spark.read.parquet(f"{processed_dir}/analytics/kpi_by_airline")
    kpi_map = {r["AIRLINE"]: r for r in df_kpi.collect()}
    
    # Aggregate fact_daily_airline by carrier
    df_recomputed = df_fda.groupBy("AIRLINE").agg(
        F.sum("total_flights").alias("total_flights"),
        F.sum("flown_flights").alias("flown_flights"),
        F.sum("cancelled_flights").alias("cancelled_flights"),
        F.sum("diverted_flights").alias("diverted_flights"),
        F.sum("delayed_arr_15").alias("delayed_arr_15"),
        F.sum("delayed_dep_15").alias("delayed_dep_15"),
        F.sum("sum_arr_delay_min").alias("sum_arr_delay_min")
    ).withColumn(
        "calc_arr_delay_rate_15", F.col("delayed_arr_15") / F.col("flown_flights")
    ).withColumn(
        "calc_cancel_rate", F.col("cancelled_flights") / F.col("total_flights")
    ).withColumn(
        "calc_divert_rate", F.col("diverted_flights") / F.col("total_flights")
    ).withColumn(
        "calc_avg_arr_delay_min", F.col("sum_arr_delay_min") / F.col("flown_flights")
    )
    
    recomp_rows = df_recomputed.collect()
    max_rate_diff = 0.0
    for r in recomp_rows:
        code = r["AIRLINE"]
        expected_r = kpi_map[code]
        
        diff_arr_rate = abs(float(r["calc_arr_delay_rate_15"]) - float(expected_r["arr_delay_rate_15"]))
        diff_cancel_rate = abs(float(r["calc_cancel_rate"]) - float(expected_r["cancel_rate"]))
        diff_divert_rate = abs(float(r["calc_divert_rate"]) - float(expected_r["divert_rate"]))
        diff_avg_delay = abs(float(r["calc_avg_arr_delay_min"]) - float(expected_r["avg_arr_delay_min"]))
        
        curr_max = max(diff_arr_rate, diff_cancel_rate, diff_divert_rate, diff_avg_delay)
        if curr_max > max_rate_diff:
            max_rate_diff = curr_max
            
    if max_rate_diff < 1e-4:
        print(f"  [PASS] All carrier ratios recomputed from fact_daily_airline equal kpi_by_airline (max diff: {max_rate_diff:.6f})")
    else:
        failures.append(f"Carrier ratio discrepancy: max diff = {max_rate_diff}")
        print(f"  [FAIL] Carrier ratio discrepancy: max diff = {max_rate_diff}")
        
    print("\n" + "=" * 70)
    if len(failures) == 0:
        print("  ALL POWERBI DATA MODEL VALIDATIONS PASSED (100%)")
        print("=" * 70)
        spark.stop()
        sys.exit(0)
    else:
        print(f"  VALIDATION FAILED WITH {len(failures)} ERROR(S):")
        for f in failures:
            print(f"  - {f}")
        print("=" * 70)
        spark.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
