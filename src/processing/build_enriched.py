#!/usr/bin/env python3
"""
src/processing/build_enriched.py - Phase 4 Flight Enrichment & Feature Engineering Pipeline.

Reads cleaned datasets from HDFS processed/ (flights_clean, airports_clean, airlines_clean).
Executes broadcast left joins to enrich airline and airport dimension details.
Generates temporal, calendar, spatial, route, and label features.
Writes output to HDFS processed/flights_enriched (Parquet, snappy, partitioned by MONTH).
"""

import os
import sys
import time
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any, List, Set

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
)


def setup_logger(project_root: Path) -> logging.Logger:
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "enrichment.log"

    logger = logging.getLogger("Phase4-Enrichment")
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


def compute_holiday_window_strings(holiday_strings: List[str], window_days: int = 3) -> List[str]:
    """Computes all ISO date strings within +/- window_days of the configured holidays."""
    holiday_dates = [date.fromisoformat(h) for h in holiday_strings]
    window_set: Set[date] = set()
    for h in holiday_dates:
        for offset in range(-window_days, window_days + 1):
            window_set.add(h + timedelta(days=offset))
    return sorted([d.isoformat() for d in window_set])


def build_enriched_pipeline() -> Dict[str, Any]:
    start_time = time.time()
    logger = setup_logger(PROJECT_ROOT)
    logger.info("============================================================")
    logger.info("Starting Phase 4 Flight Enrichment Pipeline (build_enriched)...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)

    flights_clean_path = f"{namenode_uri}{processed_dir}/flights_clean"
    airports_clean_path = f"{namenode_uri}{processed_dir}/airports_clean"
    airlines_clean_path = f"{namenode_uri}{processed_dir}/airlines_clean"
    flights_enriched_path = f"{namenode_uri}{processed_dir}/flights_enriched"

    holidays_cfg = config.get("holidays_2015", [
        "2015-01-01", "2015-01-19", "2015-02-16", "2015-05-25",
        "2015-07-03", "2015-09-07", "2015-10-12", "2015-11-11",
        "2015-11-26", "2015-12-25"
    ])
    holiday_window_dates = compute_holiday_window_strings(holidays_cfg, window_days=3)
    logger.info(f"Loaded {len(holidays_cfg)} holidays; computed {len(holiday_window_dates)} holiday window dates.")

    spark = get_spark_session(
        app_name="FlightDelay-Phase4-Enrichment",
        enable_hdfs=True,
    )
    spark.conf.set("dfs.replication", "1")

    try:
        # ----------------------------------------------------------------------
        # 1. READ CLEAN DATASETS FROM HDFS
        # ----------------------------------------------------------------------
        logger.info(f"Loading flights_clean from {flights_clean_path}...")
        df_flights = spark.read.parquet(flights_clean_path)
        input_flight_rows = df_flights.count()
        logger.info(f"Loaded {input_flight_rows:,} cleaned flight records.")

        logger.info(f"Loading airports_clean from {airports_clean_path}...")
        df_airports = spark.read.parquet(airports_clean_path)
        airports_cnt = df_airports.count()
        logger.info(f"Loaded {airports_cnt} reference airports.")

        logger.info(f"Loading airlines_clean from {airlines_clean_path}...")
        df_airlines = spark.read.parquet(airlines_clean_path)
        airlines_cnt = df_airlines.count()
        logger.info(f"Loaded {airlines_cnt} reference airlines.")

        # ----------------------------------------------------------------------
        # 2. BROADCAST LEFT JOINS (B1)
        # ----------------------------------------------------------------------
        logger.info("Executing broadcast LEFT joins on airline and airport lookups...")
        
        # Airlines lookup
        df_airl_lookup = df_airlines.select(
            F.col("AIRLINE_CODE"),
            F.col("AIRLINE_NAME")
        )
        
        # Airports lookup
        df_airp_orig = df_airports.select(
            F.col("AIRPORT_CODE").alias("orig_code"),
            F.col("AIRPORT_NAME").alias("ORIGIN_AIRPORT_NAME"),
            F.col("CITY").alias("ORIGIN_CITY"),
            F.col("STATE").alias("ORIGIN_STATE"),
            F.col("LATITUDE").alias("ORIGIN_LAT"),
            F.col("LONGITUDE").alias("ORIGIN_LON")
        )

        df_airp_dest = df_airports.select(
            F.col("AIRPORT_CODE").alias("dest_code"),
            F.col("AIRPORT_NAME").alias("DEST_AIRPORT_NAME"),
            F.col("CITY").alias("DEST_CITY"),
            F.col("STATE").alias("DEST_STATE"),
            F.col("LATITUDE").alias("DEST_LAT"),
            F.col("LONGITUDE").alias("DEST_LON")
        )

        df_joined = df_flights.join(
            F.broadcast(df_airl_lookup),
            F.col("AIRLINE") == F.col("AIRLINE_CODE"),
            how="left"
        ).drop("AIRLINE_CODE") \
        .join(
            F.broadcast(df_airp_orig),
            F.col("ORIGIN_AIRPORT") == F.col("orig_code"),
            how="left"
        ).drop("orig_code") \
        .join(
            F.broadcast(df_airp_dest),
            F.col("DESTINATION_AIRPORT") == F.col("dest_code"),
            how="left"
        ).drop("dest_code")

        # ----------------------------------------------------------------------
        # 3. FEATURE ENGINEERING (B2)
        # ----------------------------------------------------------------------
        logger.info("Building derived features, temporal attributes, and ML targets...")

        # ROUTE
        df_feats = df_joined.withColumn(
            "ROUTE",
            F.when(
                F.col("ORIGIN_AIRPORT").isNotNull() & F.col("DESTINATION_AIRPORT").isNotNull(),
                F.concat(F.col("ORIGIN_AIRPORT"), F.lit("-"), F.col("DESTINATION_AIRPORT"))
            ).otherwise(None)
        )

        # SCHED_DEP_HOUR, SCHED_ARR_HOUR
        df_feats = df_feats.withColumn(
            "SCHED_DEP_HOUR",
            F.lpad(F.col("SCHEDULED_DEPARTURE"), 4, "0").substr(1, 2).cast(IntegerType())
        ).withColumn(
            "SCHED_ARR_HOUR",
            F.lpad(F.col("SCHEDULED_ARRIVAL"), 4, "0").substr(1, 2).cast(IntegerType())
        )

        # SCHED_DEP_TS (Local airport scheduled departure timestamp)
        df_feats = df_feats.withColumn(
            "SCHED_DEP_TS",
            F.to_timestamp(
                F.concat(F.col("FLIGHT_DATE"), F.lit(" "), F.lpad(F.col("SCHEDULED_DEPARTURE"), 4, "0")),
                "yyyy-MM-dd HHmm"
            )
        )

        # DEP_TIME_BLOCK
        df_feats = df_feats.withColumn(
            "DEP_TIME_BLOCK",
            F.when(F.col("SCHED_DEP_HOUR").between(0, 5), "Night")
            .when(F.col("SCHED_DEP_HOUR").between(6, 11), "Morning")
            .when(F.col("SCHED_DEP_HOUR").between(12, 17), "Afternoon")
            .when(F.col("SCHED_DEP_HOUR").between(18, 23), "Evening")
            .otherwise(None)
        )

        # DAY_OF_WEEK_NAME, IS_WEEKEND
        df_feats = df_feats.withColumn(
            "DAY_OF_WEEK_NAME",
            F.when(F.col("DAY_OF_WEEK") == 1, "Monday")
            .when(F.col("DAY_OF_WEEK") == 2, "Tuesday")
            .when(F.col("DAY_OF_WEEK") == 3, "Wednesday")
            .when(F.col("DAY_OF_WEEK") == 4, "Thursday")
            .when(F.col("DAY_OF_WEEK") == 5, "Friday")
            .when(F.col("DAY_OF_WEEK") == 6, "Saturday")
            .when(F.col("DAY_OF_WEEK") == 7, "Sunday")
            .otherwise(None)
        ).withColumn(
            "IS_WEEKEND",
            F.when(F.col("DAY_OF_WEEK").isin(6, 7), 1).otherwise(0)
        )

        # MONTH_NAME, SEASON
        df_feats = df_feats.withColumn(
            "MONTH_NAME",
            F.when(F.col("MONTH") == 1, "January")
            .when(F.col("MONTH") == 2, "February")
            .when(F.col("MONTH") == 3, "March")
            .when(F.col("MONTH") == 4, "April")
            .when(F.col("MONTH") == 5, "May")
            .when(F.col("MONTH") == 6, "June")
            .when(F.col("MONTH") == 7, "July")
            .when(F.col("MONTH") == 8, "August")
            .when(F.col("MONTH") == 9, "September")
            .when(F.col("MONTH") == 10, "October")
            .when(F.col("MONTH") == 11, "November")
            .when(F.col("MONTH") == 12, "December")
            .otherwise(None)
        ).withColumn(
            "SEASON",
            F.when(F.col("MONTH").isin(12, 1, 2), "Winter")
            .when(F.col("MONTH").isin(3, 4, 5), "Spring")
            .when(F.col("MONTH").isin(6, 7, 8), "Summer")
            .when(F.col("MONTH").isin(9, 10, 11), "Fall")
            .otherwise(None)
        )

        # IS_HOLIDAY_WINDOW
        df_feats = df_feats.withColumn(
            "IS_HOLIDAY_WINDOW",
            F.when(F.col("FLIGHT_DATE").cast("string").isin(holiday_window_dates), 1).otherwise(0)
        )

        # DISTANCE_BAND
        df_feats = df_feats.withColumn(
            "DISTANCE_BAND",
            F.when(F.col("DISTANCE") < 500, "Short")
            .when(F.col("DISTANCE").between(500, 1500), "Medium")
            .when(F.col("DISTANCE") > 1500, "Long")
            .otherwise(None)
        )

        # LABELS (null when IS_FLOWN == 0)
        df_feats = df_feats.withColumn(
            "IS_ARR_DELAYED_15",
            F.when(F.col("IS_FLOWN") == 1,
                   F.when(F.col("ARRIVAL_DELAY") >= 15, 1).otherwise(0)
            ).otherwise(None)
        ).withColumn(
            "IS_DEP_DELAYED_15",
            F.when(F.col("IS_FLOWN") == 1,
                   F.when(F.col("DEPARTURE_DELAY") >= 15, 1).otherwise(0)
            ).otherwise(None)
        ).withColumn(
            "DELAY_CATEGORY",
            F.when(F.col("IS_FLOWN") == 1,
                   F.when(F.col("ARRIVAL_DELAY") <= 0, "On Time/Early")
                   .when(F.col("ARRIVAL_DELAY").between(1, 14), "Minor")
                   .when(F.col("ARRIVAL_DELAY").between(15, 59), "Moderate")
                   .when(F.col("ARRIVAL_DELAY").between(60, 179), "Major")
                   .when(F.col("ARRIVAL_DELAY") >= 180, "Severe")
                   .otherwise(None)
            ).otherwise(None)
        )

        # PRIMARY_DELAY_CAUSE
        c_nas = F.coalesce(F.col("AIR_SYSTEM_DELAY"), F.lit(0))
        c_sec = F.coalesce(F.col("SECURITY_DELAY"), F.lit(0))
        c_air = F.coalesce(F.col("AIRLINE_DELAY"), F.lit(0))
        c_lat = F.coalesce(F.col("LATE_AIRCRAFT_DELAY"), F.lit(0))
        c_wea = F.coalesce(F.col("WEATHER_DELAY"), F.lit(0))

        max_cause = F.greatest(c_nas, c_sec, c_air, c_lat, c_wea)
        
        # Tie count among max
        tie_count = (
            F.when(c_nas == max_cause, 1).otherwise(0) +
            F.when(c_sec == max_cause, 1).otherwise(0) +
            F.when(c_air == max_cause, 1).otherwise(0) +
            F.when(c_lat == max_cause, 1).otherwise(0) +
            F.when(c_wea == max_cause, 1).otherwise(0)
        )

        cause_expr = F.when(
            (F.col("HAS_CAUSE_DATA") == 1) & (max_cause > 0),
            F.when(tie_count > 1, "Multiple")
            .when(c_lat == max_cause, "Late Aircraft")
            .when(c_nas == max_cause, "Air System")
            .when(c_air == max_cause, "Airline")
            .when(c_wea == max_cause, "Weather")
            .when(c_sec == max_cause, "Security")
            .otherwise(None)
        ).otherwise(None)

        df_feats = df_feats.withColumn("PRIMARY_DELAY_CAUSE", cause_expr)

        # STATUS ("Cancelled", "Diverted", "Completed")
        df_feats = df_feats.withColumn(
            "STATUS",
            F.when(F.col("CANCELLED") == 1, "Cancelled")
            .when(F.col("DIVERTED") == 1, "Diverted")
            .when(F.col("IS_FLOWN") == 1, "Completed")
            .otherwise(None)
        )

        # ----------------------------------------------------------------------
        # 4. ASSERTIONS (B1)
        # ----------------------------------------------------------------------
        logger.info("Executing pipeline assertions...")
        output_rows = df_feats.count()
        assert output_rows == input_flight_rows, f"[ASSERTION FAILED] Row count changed from {input_flight_rows} to {output_rows}!"
        logger.info(f"[ASSERTION PASSED] Output row count strictly matches flights_clean: {output_rows:,} rows.")

        # Non-null IATA matching airport check
        missing_orig = df_feats.filter(
            F.col("ORIGIN_AIRPORT").isNotNull() & F.col("ORIGIN_AIRPORT_NAME").isNull()
        ).count()
        missing_dest = df_feats.filter(
            F.col("DESTINATION_AIRPORT").isNotNull() & F.col("DEST_AIRPORT_NAME").isNull()
        ).count()
        assert missing_orig == 0, f"Found {missing_orig} non-null ORIGIN_AIRPORT with missing airport lookup!"
        assert missing_dest == 0, f"Found {missing_dest} non-null DESTINATION_AIRPORT with missing airport lookup!"
        logger.info("[ASSERTION PASSED] All non-null IATA airports matched 100% with reference airports.")

        # ----------------------------------------------------------------------
        # 5. WRITE TO HDFS processed/flights_enriched (B4)
        # ----------------------------------------------------------------------
        logger.info(f"Writing flights_enriched to HDFS {flights_enriched_path} (partitioned by MONTH)...")
        write_start = time.time()
        df_feats.repartition(12, "MONTH") \
            .write \
            .partitionBy("MONTH") \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(flights_enriched_path)
        write_elapsed = time.time() - write_start
        logger.info(f"Successfully wrote flights_enriched Parquet in {write_elapsed:.2f}s.")

        total_runtime = time.time() - start_time
        logger.info("============================================================")
        logger.info(f"Phase 4 Enrichment Pipeline completed successfully in {total_runtime:.2f}s!")
        logger.info("============================================================")

        return {
            "output_rows": output_rows,
            "write_time_sec": write_elapsed,
            "total_runtime_sec": total_runtime,
        }

    finally:
        spark.stop()


if __name__ == "__main__":
    build_enriched_pipeline()
