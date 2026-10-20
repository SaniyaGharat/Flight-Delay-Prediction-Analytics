#!/usr/bin/env python3
"""
src/processing/clean_flights.py - Phase 3 Flights and Lookups Data Cleaning Pipeline.

Cleans and standardizes raw flight, airport, and airline datasets from HDFS:
- Trims whitespace and normalizes empty strings to null.
- Type casting with strict zero-loss assertion.
- Date calculation and time column normalization (2400 -> 0000).
- Applies DOT airport ID mapping for October flights.
- Identifies natural key duplicates and adds audit flags.
- Produces clean Parquet datasets in HDFS processed/ partitioned by MONTH.
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, List

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StringType,
    DateType,
)


def setup_logger(project_root: Path) -> logging.Logger:
    """Configures logging to console and logs/cleaning.log."""
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "cleaning.log"

    logger = logging.getLogger("Phase3-Cleaning")
    logger.setLevel(logging.INFO)
    logger.handlers = []

    c_handler = logging.StreamHandler(sys.stdout)
    f_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    c_handler.setFormatter(fmt)
    f_handler.setFormatter(fmt)

    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger


def clean_data_pipeline() -> Dict[str, Any]:
    """Main cleaning pipeline for flights, airports, and airlines datasets."""
    start_time = time.time()
    logger = setup_logger(PROJECT_ROOT)
    logger.info("============================================================")
    logger.info("Starting Phase 3 Flights & Lookups Data Cleaning Pipeline...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    raw_dir = hdfs_cfg.get("raw_dir", f"/user/{user}/flight_delay/raw").replace("${USER}", user)
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)

    flights_raw_path = f"{namenode_uri}{raw_dir}/flights/flights.csv"
    airports_raw_path = f"{namenode_uri}{raw_dir}/airports/airports.csv"
    airlines_raw_path = f"{namenode_uri}{raw_dir}/airlines/airlines.csv"
    airport_map_path = f"{namenode_uri}{processed_dir}/airport_id_map"

    flights_clean_path = f"{namenode_uri}{processed_dir}/flights_clean"
    airports_clean_path = f"{namenode_uri}{processed_dir}/airports_clean"
    airlines_clean_path = f"{namenode_uri}{processed_dir}/airlines_clean"

    extra_spark_cfg = {
        "spark.driver.memory": "3g",
        "spark.sql.shuffle.partitions": "24",
        "spark.hadoop.dfs.replication": "1",
    }
    spark = get_spark_session(
        app_name="FlightDelay-Phase3-Cleaning",
        enable_hdfs=True,
        extra_configs=extra_spark_cfg,
    )
    spark.conf.set("dfs.replication", "1")

    try:
        # ----------------------------------------------------------------------
        # 1. READ RAW DATASETS AND AIRPORT ID MAP
        # ----------------------------------------------------------------------
        logger.info(f"Loading raw flights from {flights_raw_path}...")
        df_flights_raw = spark.read.csv(flights_raw_path, header=True, inferSchema=False)
        total_raw_rows = df_flights_raw.count()
        logger.info(f"Raw flights row count: {total_raw_rows:,}")

        logger.info(f"Loading airport ID mapping from {airport_map_path}...")
        df_airport_map = spark.read.parquet(airport_map_path).select(
            F.col("airport_id"),
            F.col("iata_code").alias("mapped_iata")
        )
        map_count = df_airport_map.count()
        logger.info(f"Loaded {map_count} airport ID mappings from HDFS.")

        # ----------------------------------------------------------------------
        # 2. STRING TRIMMING AND EMPTY STRING TO NULL
        # ----------------------------------------------------------------------
        logger.info("Trimming strings and converting empty strings to null...")
        trimmed_cols = []
        for col_name in df_flights_raw.columns:
            trimmed_cols.append(
                F.when(F.trim(F.col(col_name)) == "", None)
                .otherwise(F.trim(F.col(col_name)))
                .alias(col_name)
            )
        df_trimmed = df_flights_raw.select(*trimmed_cols)

        # ----------------------------------------------------------------------
        # 3. EXACT DUPLICATE REMOVAL
        # ----------------------------------------------------------------------
        logger.info("Checking and removing exact duplicate records (repartitioned by MONTH)...")
        # Repartitioning by MONTH ensures memory efficiency in local mode while preserving exact duplicate semantics
        df_dedup = df_trimmed.repartition(12, "MONTH").dropDuplicates()
        dedup_count = df_dedup.count()
        exact_dupes_dropped = total_raw_rows - dedup_count
        logger.info(f"Exact duplicate rows dropped: {exact_dupes_dropped}")

        # ----------------------------------------------------------------------
        # 4. TIME COLUMNS: 2400 -> 0000 NORMALIZATION & HAS_2400_TIME FLAG
        # ----------------------------------------------------------------------
        logger.info("Normalizing time columns ('2400' -> '0000') and flagging...")
        time_cols = ["SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "WHEELS_OFF", "WHEELS_ON", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME"]
        
        # Single-pass aggregation to count 2400 occurrences
        time_2400_exprs = [
            F.sum(F.when(F.col(tc) == "2400", 1).otherwise(0)).alias(tc)
            for tc in time_cols
        ]
        norm_time_counts = df_dedup.agg(*time_2400_exprs).collect()[0].asDict()
        logger.info(f"Per-column '2400' instances normalized: {norm_time_counts}")

        has_2400_cond = F.lit(False)
        for tc in time_cols:
            has_2400_cond = has_2400_cond | (F.col(tc) == "2400")

        df_time_norm = df_dedup.withColumn(
            "HAS_2400_TIME",
            F.when(has_2400_cond, 1).otherwise(0)
        )
        for tc in time_cols:
            df_time_norm = df_time_norm.withColumn(
                tc,
                F.when(F.col(tc) == "2400", "0000").otherwise(F.col(tc))
            )

        # ----------------------------------------------------------------------
        # 5. AIRPORT CODES STANDARDIZATION & AUDIT FLAGS
        # ----------------------------------------------------------------------
        logger.info("Mapping numeric DOT airport IDs to 3-letter IATA codes...")
        df_airports_raw_kept = df_time_norm.withColumn(
            "ORIGIN_AIRPORT_RAW", F.col("ORIGIN_AIRPORT")
        ).withColumn(
            "DESTINATION_AIRPORT_RAW", F.col("DESTINATION_AIRPORT")
        )

        # Join origin mapping (broadcast small map table)
        df_orig_mapped = df_airports_raw_kept.join(
            F.broadcast(df_airport_map.alias("orig_m")),
            F.col("ORIGIN_AIRPORT_RAW") == F.col("orig_m.airport_id"),
            how="left"
        ).withColumn(
            "ORIGIN_ID_MAPPED",
            F.when(F.col("orig_m.mapped_iata").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "ORIGIN_UNMAPPED",
            F.when(F.col("ORIGIN_AIRPORT_RAW").rlike(r"^\d+$") & F.col("orig_m.mapped_iata").isNull(), 1).otherwise(0)
        ).withColumn(
            "ORIGIN_AIRPORT",
            F.when(F.col("ORIGIN_AIRPORT_RAW").rlike(r"^[A-Z]{3}$"), F.col("ORIGIN_AIRPORT_RAW"))
            .when(F.col("orig_m.mapped_iata").isNotNull(), F.col("orig_m.mapped_iata"))
            .otherwise(None)
        ).drop("orig_m.airport_id", "orig_m.mapped_iata")

        # Join destination mapping (broadcast small map table)
        df_dest_mapped = df_orig_mapped.join(
            F.broadcast(df_airport_map.alias("dest_m")),
            F.col("DESTINATION_AIRPORT_RAW") == F.col("dest_m.airport_id"),
            how="left"
        ).withColumn(
            "DEST_ID_MAPPED",
            F.when(F.col("dest_m.mapped_iata").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "DEST_UNMAPPED",
            F.when(F.col("DESTINATION_AIRPORT_RAW").rlike(r"^\d+$") & F.col("dest_m.mapped_iata").isNull(), 1).otherwise(0)
        ).withColumn(
            "DESTINATION_AIRPORT",
            F.when(F.col("DESTINATION_AIRPORT_RAW").rlike(r"^[A-Z]{3}$"), F.col("DESTINATION_AIRPORT_RAW"))
            .when(F.col("dest_m.mapped_iata").isNotNull(), F.col("dest_m.mapped_iata"))
            .otherwise(None)
        ).drop("dest_m.airport_id", "dest_m.mapped_iata")

        # ----------------------------------------------------------------------
        # 6. TYPE CASTING WITH STRICT ASSERTION
        # ----------------------------------------------------------------------
        logger.info("Casting numeric and duration columns to IntegerType with zero-loss assertion...")
        int_cast_columns = [
            "YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "FLIGHT_NUMBER", "DISTANCE", "CANCELLED", "DIVERTED",
            "DEPARTURE_DELAY", "ARRIVAL_DELAY", "TAXI_OUT", "TAXI_IN", "AIR_TIME", "ELAPSED_TIME", "SCHEDULED_TIME",
            "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"
        ]

        # Single-pass assertion check across all 20 columns
        cast_check_exprs = [
            F.sum(F.when(F.col(c).isNotNull() & F.col(c).cast(IntegerType()).isNull(), 1).otherwise(0)).alias(c)
            for c in int_cast_columns
        ]
        failed_cast_counts = df_dest_mapped.agg(*cast_check_exprs).collect()[0].asDict()
        for col_name, failed_cnt in failed_cast_counts.items():
            if failed_cnt > 0:
                logger.error(f"[FATAL ASSERTION ERROR] Column {col_name} produced {failed_cnt} nulls upon casting to int!")
                raise AssertionError(f"Type cast to int failed for {failed_cnt} rows in column {col_name}")

        df_casted = df_dest_mapped
        for c in int_cast_columns:
            df_casted = df_casted.withColumn(c, F.col(c).cast(IntegerType()))

        logger.info(f"[ASSERTION PASSED] All {len(int_cast_columns)} columns casted to int with 0 new nulls created.")

        # ----------------------------------------------------------------------
        # 7. FLIGHT_DATE AND DESCRIPTIVE FLAGS
        # ----------------------------------------------------------------------
        logger.info("Computing FLIGHT_DATE and classification flags...")
        df_flags = df_casted.withColumn(
            "FLIGHT_DATE",
            F.to_date(
                F.concat_ws("-", F.col("YEAR"), F.lpad(F.col("MONTH").cast("string"), 2, "0"), F.lpad(F.col("DAY").cast("string"), 2, "0")),
                "yyyy-MM-dd"
            )
        ).withColumn(
            "IS_FLOWN",
            F.when((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0), 1).otherwise(0)
        ).withColumn(
            "HAS_CAUSE_DATA",
            F.when(
                F.col("AIR_SYSTEM_DELAY").isNotNull() | F.col("SECURITY_DELAY").isNotNull() |
                F.col("AIRLINE_DELAY").isNotNull() | F.col("LATE_AIRCRAFT_DELAY").isNotNull() |
                F.col("WEATHER_DELAY").isNotNull(),
                1
            ).otherwise(0)
        ).withColumn(
            "CANCELLATION_REASON_DESC",
            F.when(F.col("CANCELLATION_REASON") == "A", "Airline/Carrier")
            .when(F.col("CANCELLATION_REASON") == "B", "Weather")
            .when(F.col("CANCELLATION_REASON") == "C", "National Air System")
            .when(F.col("CANCELLATION_REASON") == "D", "Security")
            .otherwise(None)
        )

        # ----------------------------------------------------------------------
        # 8. NATURAL-KEY DUPLICATE FLAGGING (DO NOT DROP)
        # ----------------------------------------------------------------------
        logger.info("Identifying natural key duplicates...")
        nat_key = ["YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT_RAW", "SCHEDULED_DEPARTURE"]
        w_nat = Window.partitionBy(*nat_key)

        df_nat = df_flags.withColumn("nat_count", F.count("*").over(w_nat)) \
            .withColumn("DUP_NATURAL_KEY", F.when(F.col("nat_count") > 1, 1).otherwise(0)) \
            .drop("nat_count")

        nat_dupe_rows = df_nat.filter(F.col("DUP_NATURAL_KEY") == 1).select(
            "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW",
            "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAIL_NUMBER"
        ).collect()

        logger.info(f"Natural-key duplicate rows identified: {len(nat_dupe_rows)}")
        for i, row in enumerate(nat_dupe_rows, 1):
            logger.info(f"  Natural Dupe #{i}: Date={row['FLIGHT_DATE']} Airline={row['AIRLINE']} Flight={row['FLIGHT_NUMBER']} Origin={row['ORIGIN_AIRPORT_RAW']} Dest={row['DESTINATION_AIRPORT_RAW']} SchedDep={row['SCHEDULED_DEPARTURE']} DepTime={row['DEPARTURE_TIME']} Delay={row['DEPARTURE_DELAY']} Tail={row['TAIL_NUMBER']}")

        # ----------------------------------------------------------------------
        # 9. AUDIT NULL SCHEDULED_TIME ROWS
        # ----------------------------------------------------------------------
        sched_time_null_rows = df_nat.filter(F.col("SCHEDULED_TIME").isNull()).select(
            "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW",
            "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "DISTANCE"
        ).collect()
        logger.info(f"Auditing null SCHEDULED_TIME rows ({len(sched_time_null_rows)} total):")
        for i, r in enumerate(sched_time_null_rows, 1):
            logger.info(f"  Null SchedTime #{i}: Date={r['FLIGHT_DATE']} Airline={r['AIRLINE']} Flight={r['FLIGHT_NUMBER']} Route={r['ORIGIN_AIRPORT_RAW']}->{r['DESTINATION_AIRPORT_RAW']} SchedDep={r['SCHEDULED_DEPARTURE']} SchedArr={r['SCHEDULED_ARRIVAL']} Dist={r['DISTANCE']}")

        # Re-order and clean schema
        final_flights_columns = [
            "YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER",
            "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW",
            "ORIGIN_ID_MAPPED", "DEST_ID_MAPPED", "ORIGIN_UNMAPPED", "DEST_UNMAPPED",
            "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT", "WHEELS_OFF",
            "SCHEDULED_TIME", "ELAPSED_TIME", "AIR_TIME", "DISTANCE", "WHEELS_ON", "TAXI_IN",
            "SCHEDULED_ARRIVAL", "ARRIVAL_TIME", "ARRIVAL_DELAY",
            "DIVERTED", "CANCELLED", "CANCELLATION_REASON", "CANCELLATION_REASON_DESC",
            "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY",
            "IS_FLOWN", "HAS_CAUSE_DATA", "HAS_2400_TIME", "DUP_NATURAL_KEY"
        ]

        df_flights_clean = df_nat.select(*final_flights_columns)

        # ----------------------------------------------------------------------
        # 10. WRITE FLIGHTS CLEAN TO HDFS (PARQUET, SNAPPY, PARTITIONED BY MONTH)
        # ----------------------------------------------------------------------
        logger.info(f"Writing clean flights to HDFS {flights_clean_path} (partitioned by MONTH)...")
        write_start = time.time()
        df_flights_clean.repartition(12, "MONTH") \
            .write \
            .partitionBy("MONTH") \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(flights_clean_path)
        logger.info(f"Successfully wrote flights_clean Parquet in {time.time() - write_start:.2f}s.")

        # ----------------------------------------------------------------------
        # 11. CLEAN AIRPORTS LOOKUP TABLE
        # ----------------------------------------------------------------------
        logger.info(f"Cleaning airports lookup from {airports_raw_path}...")
        df_airports_raw = spark.read.csv(airports_raw_path, header=True, inferSchema=False)
        df_airports_clean = df_airports_raw.select(
            F.trim(F.col("IATA_CODE")).alias("AIRPORT_CODE"),
            F.trim(F.col("AIRPORT")).alias("AIRPORT_NAME"),
            F.trim(F.col("CITY")).alias("CITY"),
            F.trim(F.col("STATE")).alias("STATE"),
            F.trim(F.col("COUNTRY")).alias("COUNTRY"),
            F.col("LATITUDE").cast(DoubleType()).alias("LATITUDE"),
            F.col("LONGITUDE").cast(DoubleType()).alias("LONGITUDE")
        ).withColumn(
            "HAS_COORDS",
            F.when(F.col("LATITUDE").isNotNull() & F.col("LONGITUDE").isNotNull(), 1).otherwise(0)
        )
        airports_cnt = df_airports_clean.count()
        logger.info(f"Cleaned airports count: {airports_cnt} records. Writing to {airports_clean_path}...")
        df_airports_clean.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(airports_clean_path)

        # ----------------------------------------------------------------------
        # 12. CLEAN AIRLINES LOOKUP TABLE
        # ----------------------------------------------------------------------
        logger.info(f"Cleaning airlines lookup from {airlines_raw_path}...")
        df_airlines_raw = spark.read.csv(airlines_raw_path, header=True, inferSchema=False)
        df_airlines_clean = df_airlines_raw.select(
            F.trim(F.col("IATA_CODE")).alias("AIRLINE_CODE"),
            F.trim(F.col("AIRLINE")).alias("AIRLINE_NAME")
        )
        airlines_cnt = df_airlines_clean.count()
        logger.info(f"Cleaned airlines count: {airlines_cnt} records. Writing to {airlines_clean_path}...")
        df_airlines_clean.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(airlines_clean_path)

        total_runtime = time.time() - start_time
        logger.info("============================================================")
        logger.info(f"Phase 3 Cleaning Pipeline completed successfully in {total_runtime:.2f}s!")
        logger.info("============================================================")

        return {
            "total_raw_rows": total_raw_rows,
            "exact_dupes_dropped": exact_dupes_dropped,
            "final_clean_rows": total_raw_rows - exact_dupes_dropped,
            "norm_time_counts": norm_time_counts,
            "natural_dupes": len(nat_dupe_rows),
            "null_sched_time": len(sched_time_null_rows),
            "airports_count": airports_cnt,
            "airlines_count": airlines_cnt,
            "runtime_sec": total_runtime,
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    clean_data_pipeline()
