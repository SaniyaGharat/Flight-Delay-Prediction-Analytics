#!/usr/bin/env python3
"""
src/processing/build_powerbi_model.py

Builds star-schema dimensional model for PowerBI:
- Dimensions: dim_date, dim_airline, dim_airport (with UNMAPPED row)
- Facts:
  - fact_daily_airline
  - fact_daily_origin
  - fact_daily_dest
  - fact_airline_dow_hour
  - fact_route_month
  - fact_cause_daily_airline (long)
  - fact_cancel_daily_airline_reason
Writes:
- Parquet to HDFS: processed/powerbi/<table>
- CSV to Local: output/powerbi/<table>.csv (single file, UTF-8, header, ISO dates, dot decimals, empty string for nulls)
"""

import os
import sys
import yaml
import time
from pathlib import Path
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType, DateType
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

def load_config():
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_spark_session(app_name="build_powerbi_model"):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[6]") \
        .config("spark.driver.memory", "5g") \
        .config("spark.sql.shuffle.partitions", 24) \
        .config("spark.hadoop.dfs.replication", 1) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def save_table(df, table_name, hdfs_base, local_csv_dir):
    print(f"\n[INFO] Saving table: {table_name}...")
    hdfs_path = f"{hdfs_base}/{table_name}"
    
    # 1. Write Parquet to HDFS (coalesce(1))
    df.coalesce(1).write.mode("overwrite").parquet(hdfs_path)
    print(f"  -> Written Parquet to HDFS: {hdfs_path}")
    
    # 2. Write CSV to local directory (single file, UTF-8, header, ISO dates, dot decimals, empty string for nulls)
    local_csv_path = local_csv_dir / f"{table_name}.csv"
    pdf = df.toPandas()
    
    # Ensure date columns are formatted as YYYY-MM-DD
    for col in pdf.columns:
        if "date" in col.lower():
            pdf[col] = pdf[col].astype(str)
            
    pdf.to_csv(
        local_csv_path,
        index=False,
        header=True,
        encoding="utf-8",
        date_format="%Y-%m-%d",
        na_rep=""
    )
    print(f"  -> Written local CSV: {local_csv_path} ({len(pdf):,} rows, {local_csv_path.stat().st_size:,} bytes)")
    return len(pdf)

def main():
    t0 = time.perf_counter()
    config = load_config()
    
    local_csv_dir = PROJECT_ROOT / config.get("local_paths", {}).get("output_powerbi", "output/powerbi")
    local_csv_dir.mkdir(parents=True, exist_ok=True)
    
    processed_dir = config.get("hdfs", {}).get("processed_dir", "/user/saniya/flight_delay/processed")
    if not processed_dir.startswith("hdfs://"):
        processed_dir = f"hdfs://localhost:9000{processed_dir}"
    powerbi_hdfs_base = f"{processed_dir}/powerbi"
    
    spark = get_spark_session()
    
    print("=" * 70)
    print("  PHASE 7: BUILDING POWERBI STAR-SCHEMA DATA MODEL")
    print(f"  HDFS Target:  {powerbi_hdfs_base}")
    print(f"  Local Target: {local_csv_dir}")
    print("=" * 70)
    
    # Read inputs
    flights_enriched_path = f"{processed_dir}/flights_enriched"
    dim_date_path = f"{processed_dir}/dim_date"
    airlines_clean_path = f"{processed_dir}/airlines_clean"
    airports_clean_path = f"{processed_dir}/airports_clean"
    
    df_enriched = spark.read.parquet(flights_enriched_path)
    df_dim_date = spark.read.parquet(dim_date_path)
    df_airlines = spark.read.parquet(airlines_clean_path)
    df_airports = spark.read.parquet(airports_clean_path)
    
    row_counts = {}
    
    # =========================================================================
    # DIMENSIONS
    # =========================================================================
    
    # 1. dim_date (365 rows)
    dim_date = df_dim_date.select(
        "DATE", "YEAR", "MONTH", "MONTH_NAME", "DAY", "DAY_OF_WEEK",
        "DAY_OF_WEEK_NAME", "WEEK_OF_YEAR", "IS_WEEKEND", "IS_HOLIDAY_WINDOW"
    ).orderBy("DATE")
    row_counts["dim_date"] = save_table(dim_date, "dim_date", powerbi_hdfs_base, local_csv_dir)
    assert row_counts["dim_date"] == 365, f"dim_date must have 365 rows, got {row_counts['dim_date']}"
    
    # 2. dim_airline (14 rows)
    dim_airline = df_airlines.select(
        F.col("AIRLINE_CODE").alias("code"),
        F.col("AIRLINE_NAME").alias("name")
    ).orderBy("code")
    row_counts["dim_airline"] = save_table(dim_airline, "dim_airline", powerbi_hdfs_base, local_csv_dir)
    assert row_counts["dim_airline"] == 14, f"dim_airline must have 14 rows, got {row_counts['dim_airline']}"
    
    # 3. dim_airport (322 + 1 UNMAPPED = 323 rows)
    # Schema: code, name, city, state, lat, lon, HAS_COORDS
    airports_mapped = df_airports.select(
        F.col("AIRPORT_CODE").alias("code"),
        F.col("AIRPORT_NAME").alias("name"),
        F.col("CITY").alias("city"),
        F.col("STATE").alias("state"),
        F.col("LATITUDE").cast("double").alias("lat"),
        F.col("LONGITUDE").cast("double").alias("lon"),
        F.col("HAS_COORDS").cast("integer").alias("HAS_COORDS")
    )
    
    unmapped_airport_schema = airports_mapped.schema
    unmapped_row = [("UNMAPPED", "Unmapped / Unknown Airport", "Unknown", "NA", None, None, 0)]
    df_unmapped = spark.createDataFrame(unmapped_row, unmapped_airport_schema)
    
    dim_airport = airports_mapped.unionByName(df_unmapped).orderBy("code")
    row_counts["dim_airport"] = save_table(dim_airport, "dim_airport", powerbi_hdfs_base, local_csv_dir)
    assert row_counts["dim_airport"] == 323, f"dim_airport must have 323 rows, got {row_counts['dim_airport']}"
    
    # =========================================================================
    # PREPARE BASE FLIGHTS
    # =========================================================================
    # Map null airport keys to 'UNMAPPED'
    df_f = df_enriched.withColumn(
        "ORIGIN_AIRPORT", F.coalesce(F.col("ORIGIN_AIRPORT"), F.lit("UNMAPPED"))
    ).withColumn(
        "DEST_AIRPORT", F.coalesce(F.col("DESTINATION_AIRPORT"), F.lit("UNMAPPED"))
    ).withColumn(
        "ROUTE", F.concat_ws("-", F.coalesce(F.col("ORIGIN_AIRPORT"), F.lit("UNMAPPED")), F.coalesce(F.col("DESTINATION_AIRPORT"), F.lit("UNMAPPED")))
    )
    
    # =========================================================================
    # FACTS
    # =========================================================================
    
    # Helper for standard fact measures
    def get_standard_fact(df, group_cols):
        return df.groupBy(*group_cols).agg(
            F.count(F.lit(1)).cast("long").alias("total_flights"),
            F.sum("IS_FLOWN").cast("long").alias("flown_flights"),
            F.sum("CANCELLED").cast("long").alias("cancelled_flights"),
            F.sum("DIVERTED").cast("long").alias("diverted_flights"),
            F.sum("IS_ARR_DELAYED_15").cast("long").alias("delayed_arr_15"),
            F.sum("IS_DEP_DELAYED_15").cast("long").alias("delayed_dep_15"),
            F.sum(F.when(F.col("IS_FLOWN") == 1, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_arr_delay_min"),
            F.sum(F.when(F.col("DEPARTURE_DELAY").isNotNull(), F.col("DEPARTURE_DELAY")).otherwise(0)).cast("double").alias("sum_dep_delay_min"),
            F.sum(F.when(F.col("ARRIVAL_DELAY") > 0, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_delay_minutes_pos"),
            F.sum(F.coalesce(F.col("AIR_SYSTEM_DELAY"), F.lit(0))).cast("double").alias("sum_cause_air_system"),
            F.sum(F.coalesce(F.col("SECURITY_DELAY"), F.lit(0))).cast("double").alias("sum_cause_security"),
            F.sum(F.coalesce(F.col("AIRLINE_DELAY"), F.lit(0))).cast("double").alias("sum_cause_airline"),
            F.sum(F.coalesce(F.col("LATE_AIRCRAFT_DELAY"), F.lit(0))).cast("double").alias("sum_cause_late_aircraft"),
            F.sum(F.coalesce(F.col("WEATHER_DELAY"), F.lit(0))).cast("double").alias("sum_cause_weather")
        )
    
    # 1. fact_daily_airline (FLIGHT_DATE, AIRLINE)
    fact_daily_airline = get_standard_fact(df_f, ["FLIGHT_DATE", "AIRLINE"]).orderBy("FLIGHT_DATE", "AIRLINE")
    row_counts["fact_daily_airline"] = save_table(fact_daily_airline, "fact_daily_airline", powerbi_hdfs_base, local_csv_dir)
    
    # 2. fact_daily_origin (FLIGHT_DATE, ORIGIN_AIRPORT)
    fact_daily_origin = get_standard_fact(df_f, ["FLIGHT_DATE", "ORIGIN_AIRPORT"]).orderBy("FLIGHT_DATE", "ORIGIN_AIRPORT")
    row_counts["fact_daily_origin"] = save_table(fact_daily_origin, "fact_daily_origin", powerbi_hdfs_base, local_csv_dir)
    
    # 3. fact_daily_dest (FLIGHT_DATE, DEST_AIRPORT)
    fact_daily_dest = get_standard_fact(df_f, ["FLIGHT_DATE", "DEST_AIRPORT"]).orderBy("FLIGHT_DATE", "DEST_AIRPORT")
    row_counts["fact_daily_dest"] = save_table(fact_daily_dest, "fact_daily_dest", powerbi_hdfs_base, local_csv_dir)
    
    # 4. fact_airline_dow_hour (AIRLINE, DAY_OF_WEEK, SCHED_DEP_HOUR)
    fact_airline_dow_hour = df_f.groupBy("AIRLINE", "DAY_OF_WEEK", "SCHED_DEP_HOUR").agg(
        F.count(F.lit(1)).cast("long").alias("total_flights"),
        F.sum("IS_FLOWN").cast("long").alias("flown_flights"),
        F.sum("CANCELLED").cast("long").alias("cancelled_flights"),
        F.sum("DIVERTED").cast("long").alias("diverted_flights"),
        F.sum("IS_ARR_DELAYED_15").cast("long").alias("delayed_arr_15"),
        F.sum("IS_DEP_DELAYED_15").cast("long").alias("delayed_dep_15"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_arr_delay_min"),
        F.sum(F.when(F.col("DEPARTURE_DELAY").isNotNull(), F.col("DEPARTURE_DELAY")).otherwise(0)).cast("double").alias("sum_dep_delay_min"),
        F.sum(F.when(F.col("ARRIVAL_DELAY") > 0, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_delay_minutes_pos")
    ).orderBy("AIRLINE", "DAY_OF_WEEK", "SCHED_DEP_HOUR")
    row_counts["fact_airline_dow_hour"] = save_table(fact_airline_dow_hour, "fact_airline_dow_hour", powerbi_hdfs_base, local_csv_dir)
    
    # 5. fact_route_month (ROUTE, ORIGIN, DEST, MONTH)
    df_fr = df_f.withColumnRenamed("ORIGIN_AIRPORT", "ORIGIN") \
                .withColumnRenamed("DEST_AIRPORT", "DEST")
    fact_route_month = df_fr.groupBy("ROUTE", "ORIGIN", "DEST", "MONTH").agg(
        F.count(F.lit(1)).cast("long").alias("total_flights"),
        F.sum("IS_FLOWN").cast("long").alias("flown_flights"),
        F.sum("CANCELLED").cast("long").alias("cancelled_flights"),
        F.sum("DIVERTED").cast("long").alias("diverted_flights"),
        F.sum("IS_ARR_DELAYED_15").cast("long").alias("delayed_arr_15"),
        F.sum("IS_DEP_DELAYED_15").cast("long").alias("delayed_dep_15"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_arr_delay_min"),
        F.sum(F.when(F.col("DEPARTURE_DELAY").isNotNull(), F.col("DEPARTURE_DELAY")).otherwise(0)).cast("double").alias("sum_dep_delay_min"),
        F.sum(F.when(F.col("ARRIVAL_DELAY") > 0, F.col("ARRIVAL_DELAY")).otherwise(0)).cast("double").alias("sum_delay_minutes_pos")
    ).orderBy("ROUTE", "MONTH")
    row_counts["fact_route_month"] = save_table(fact_route_month, "fact_route_month", powerbi_hdfs_base, local_csv_dir)
    
    # 6. fact_cause_daily_airline (long: FLIGHT_DATE, AIRLINE, CAUSE, MINUTES, FLIGHTS)
    cause_defs = [
        ("Air System", "AIR_SYSTEM_DELAY"),
        ("Security", "SECURITY_DELAY"),
        ("Airline", "AIRLINE_DELAY"),
        ("Late Aircraft", "LATE_AIRCRAFT_DELAY"),
        ("Weather", "WEATHER_DELAY"),
    ]
    cause_dfs = []
    for cause_name, col_name in cause_defs:
        cdf = df_f.filter(F.col(col_name) > 0).groupBy("FLIGHT_DATE", "AIRLINE").agg(
            F.lit(cause_name).alias("CAUSE"),
            F.sum(col_name).cast("double").alias("MINUTES"),
            F.count(F.lit(1)).cast("long").alias("FLIGHTS")
        )
        cause_dfs.append(cdf)
        
    fact_cause_daily_airline = cause_dfs[0]
    for cdf in cause_dfs[1:]:
        fact_cause_daily_airline = fact_cause_daily_airline.unionByName(cdf)
        
    fact_cause_daily_airline = fact_cause_daily_airline.select(
        "FLIGHT_DATE", "AIRLINE", "CAUSE", "MINUTES", "FLIGHTS"
    ).orderBy("FLIGHT_DATE", "AIRLINE", "CAUSE")
    row_counts["fact_cause_daily_airline"] = save_table(fact_cause_daily_airline, "fact_cause_daily_airline", powerbi_hdfs_base, local_csv_dir)
    
    # 7. fact_cancel_daily_airline_reason (FLIGHT_DATE, AIRLINE, CANCELLATION_REASON, CANCELLED_FLIGHTS)
    # Long/group format of cancelled flights
    fact_cancel_daily_airline_reason = df_f.filter(F.col("CANCELLED") == 1).groupBy(
        "FLIGHT_DATE", "AIRLINE", F.coalesce(F.col("CANCELLATION_REASON"), F.lit("Unknown")).alias("CANCELLATION_REASON")
    ).agg(
        F.count(F.lit(1)).cast("long").alias("CANCELLED_FLIGHTS")
    ).orderBy("FLIGHT_DATE", "AIRLINE", "CANCELLATION_REASON")
    row_counts["fact_cancel_daily_airline_reason"] = save_table(fact_cancel_daily_airline_reason, "fact_cancel_daily_airline_reason", powerbi_hdfs_base, local_csv_dir)
    
    elapsed = time.perf_counter() - t0
    print("\n" + "=" * 70)
    print(f"[SUCCESS] All 10 PowerBI tables generated successfully in {elapsed:.2f}s!")
    print("=" * 70)
    for tbl, cnt in row_counts.items():
        print(f"  - {tbl:<32}: {cnt:>10,} rows")
    print("=" * 70)
    
    spark.stop()

if __name__ == "__main__":
    main()
