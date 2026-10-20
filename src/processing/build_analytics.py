"""
src/processing/build_analytics.py - Generates analytical KPI and breakdown tables.

Reads flights_enriched from HDFS processed/, computes all required aggregated analytics tables
from a single cached DataFrame projection, and writes Parquet (snappy, replication 1, coalesce(1))
to processed/analytics/<table>/.
"""

import os
import sys
import time
import json
import logging
from pathlib import Path

PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def setup_logger():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("build_analytics")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_dir / "analytics.log", mode="a", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    return logger


def get_base_metrics():
    """Returns standard additive aggregation expressions across all dimension tables."""
    return [
        F.count("*").alias("total_flights"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, 1).otherwise(0)).alias("flown_flights"),
        F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("cancelled_flights"),
        F.sum(F.when(F.col("DIVERTED") == 1, 1).otherwise(0)).alias("diverted_flights"),
        F.sum(F.coalesce(F.col("IS_ARR_DELAYED_15"), F.lit(0))).alias("delayed_arr_15"),
        F.sum(F.coalesce(F.col("IS_DEP_DELAYED_15"), F.lit(0))).alias("delayed_dep_15"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, F.col("ARRIVAL_DELAY")).otherwise(0)).alias("sum_arr_delay_min"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, F.col("DEPARTURE_DELAY")).otherwise(0)).alias("sum_dep_delay_min"),
        F.sum(F.when(F.col("IS_FLOWN") == 1, F.greatest(F.col("ARRIVAL_DELAY"), F.lit(0))).otherwise(0)).alias("sum_delay_minutes_pos"),
        F.sum(F.when(F.col("HAS_CAUSE_DATA") == 1, F.col("AIR_SYSTEM_DELAY")).otherwise(0)).alias("sum_cause_air_system"),
        F.sum(F.when(F.col("HAS_CAUSE_DATA") == 1, F.col("SECURITY_DELAY")).otherwise(0)).alias("sum_cause_security"),
        F.sum(F.when(F.col("HAS_CAUSE_DATA") == 1, F.col("AIRLINE_DELAY")).otherwise(0)).alias("sum_cause_airline"),
        F.sum(F.when(F.col("HAS_CAUSE_DATA") == 1, F.col("LATE_AIRCRAFT_DELAY")).otherwise(0)).alias("sum_cause_late_aircraft"),
        F.sum(F.when(F.col("HAS_CAUSE_DATA") == 1, F.col("WEATHER_DELAY")).otherwise(0)).alias("sum_cause_weather"),
        F.sum(F.when((F.col("IS_FLOWN") == 1) & (F.col("ARRIVAL_DELAY") >= 15), F.col("ARRIVAL_DELAY")).otherwise(0)).alias("sum_arr_delay_when_delayed_min")
    ]


def add_derived_metrics(df):
    """Calculates derived rates, averages, and cause shares from additive measures."""
    cause_total = (
        F.col("sum_cause_air_system") +
        F.col("sum_cause_security") +
        F.col("sum_cause_airline") +
        F.col("sum_cause_late_aircraft") +
        F.col("sum_cause_weather")
    )
    
    return df \
        .withColumn("cancel_rate", F.when(F.col("total_flights") > 0, F.round(F.col("cancelled_flights") / F.col("total_flights"), 6))) \
        .withColumn("divert_rate", F.when(F.col("total_flights") > 0, F.round(F.col("diverted_flights") / F.col("total_flights"), 6))) \
        .withColumn("arr_delay_rate_15", F.when(F.col("flown_flights") > 0, F.round(F.col("delayed_arr_15") / F.col("flown_flights"), 6))) \
        .withColumn("on_time_rate", F.when(F.col("flown_flights") > 0, F.round(F.lit(1.0) - (F.col("delayed_arr_15") / F.col("flown_flights")), 6))) \
        .withColumn("dep_delay_rate_15", F.when(F.col("flown_flights") > 0, F.round(F.col("delayed_dep_15") / F.col("flown_flights"), 6))) \
        .withColumn("avg_arr_delay_min", F.when(F.col("flown_flights") > 0, F.round(F.col("sum_arr_delay_min") / F.col("flown_flights"), 4))) \
        .withColumn("avg_arr_delay_when_delayed", F.when(F.col("delayed_arr_15") > 0, F.round(F.col("sum_arr_delay_when_delayed_min") / F.col("delayed_arr_15"), 4))) \
        .withColumn("share_cause_air_system", F.when(cause_total > 0, F.round(F.col("sum_cause_air_system") / cause_total, 6))) \
        .withColumn("share_cause_security", F.when(cause_total > 0, F.round(F.col("sum_cause_security") / cause_total, 6))) \
        .withColumn("share_cause_airline", F.when(cause_total > 0, F.round(F.col("sum_cause_airline") / cause_total, 6))) \
        .withColumn("share_cause_late_aircraft", F.when(cause_total > 0, F.round(F.col("sum_cause_late_aircraft") / cause_total, 6))) \
        .withColumn("share_cause_weather", F.when(cause_total > 0, F.round(F.col("sum_cause_weather") / cause_total, 6))) \
        .drop("sum_arr_delay_when_delayed_min")


def build_analytics_pipeline():
    start_time = time.perf_counter()
    logger = setup_logger()
    logger.info("============================================================")
    logger.info("Starting Phase 5 Delay Analytics Aggregation Pipeline...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)
    
    analytics_base = f"{namenode_uri}{processed_dir}/analytics"
    enriched_path = f"{namenode_uri}{processed_dir}/flights_enriched"

    thresholds = config.get("analytics_thresholds", {"min_flown_airport": 1000, "min_flown_route": 500})
    min_flown_airport = thresholds.get("min_flown_airport", 1000)
    min_flown_route = thresholds.get("min_flown_route", 500)
    logger.info(f"Loaded volume thresholds: min_flown_airport={min_flown_airport}, min_flown_route={min_flown_route}")

    spark = get_spark_session("build_analytics")

    try:
        # 1. Read flights_enriched projection
        logger.info(f"Loading flights_enriched from {enriched_path}...")
        needed_cols = [
            "YEAR", "MONTH", "DAY", "FLIGHT_DATE", "DAY_OF_WEEK", "DAY_OF_WEEK_NAME", "MONTH_NAME",
            "AIRLINE", "AIRLINE_NAME",
            "ORIGIN_AIRPORT", "ORIGIN_AIRPORT_NAME", "ORIGIN_CITY", "ORIGIN_STATE", "ORIGIN_LAT", "ORIGIN_LON", "ORIGIN_UNMAPPED",
            "DESTINATION_AIRPORT", "DEST_AIRPORT_NAME", "DEST_CITY", "DEST_STATE", "DEST_LAT", "DEST_LON", "DEST_UNMAPPED",
            "ROUTE", "DISTANCE", "SCHED_DEP_HOUR", "DEP_TIME_BLOCK",
            "DISTANCE_BAND", "SEASON", "IS_WEEKEND", "IS_HOLIDAY_WINDOW",
            "ARRIVAL_DELAY", "DEPARTURE_DELAY", "CANCELLED", "DIVERTED",
            "CANCELLATION_REASON", "CANCELLATION_REASON_DESC",
            "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY",
            "IS_FLOWN", "HAS_CAUSE_DATA", "IS_ARR_DELAYED_15", "IS_DEP_DELAYED_15", "DELAY_CATEGORY"
        ]
        
        df_raw = spark.read.parquet(enriched_path).select(needed_cols)
        
        # Normalize UNMAPPED airport keys to literal 'UNMAPPED'
        df = df_raw.withColumn(
            "ORIGIN_AIRPORT_KEY",
            F.when(F.col("ORIGIN_UNMAPPED") == 1, F.lit("UNMAPPED")).otherwise(F.col("ORIGIN_AIRPORT"))
        ).withColumn(
            "ORIGIN_NAME_KEY",
            F.when(F.col("ORIGIN_UNMAPPED") == 1, F.lit("Unmapped Airport Code")).otherwise(F.col("ORIGIN_AIRPORT_NAME"))
        ).withColumn(
            "ORIGIN_CITY_KEY",
            F.when(F.col("ORIGIN_UNMAPPED") == 1, F.lit("Unknown")).otherwise(F.col("ORIGIN_CITY"))
        ).withColumn(
            "ORIGIN_STATE_KEY",
            F.when(F.col("ORIGIN_UNMAPPED") == 1, F.lit("NA")).otherwise(F.col("ORIGIN_STATE"))
        ).withColumn(
            "DEST_AIRPORT_KEY",
            F.when(F.col("DEST_UNMAPPED") == 1, F.lit("UNMAPPED")).otherwise(F.col("DESTINATION_AIRPORT"))
        ).withColumn(
            "DEST_NAME_KEY",
            F.when(F.col("DEST_UNMAPPED") == 1, F.lit("Unmapped Airport Code")).otherwise(F.col("DEST_AIRPORT_NAME"))
        ).withColumn(
            "DEST_CITY_KEY",
            F.when(F.col("DEST_UNMAPPED") == 1, F.lit("Unknown")).otherwise(F.col("DEST_CITY"))
        ).withColumn(
            "DEST_STATE_KEY",
            F.when(F.col("DEST_UNMAPPED") == 1, F.lit("NA")).otherwise(F.col("DEST_STATE"))
        ).withColumn(
            "ROUTE_KEY",
            F.when((F.col("ORIGIN_UNMAPPED") == 1) | (F.col("DEST_UNMAPPED") == 1), F.lit("UNMAPPED")).otherwise(F.col("ROUTE"))
        ).cache()

        total_cnt = df.count()
        logger.info(f"Cached analytics base projection: {total_cnt:,} rows.")
        assert total_cnt == 5819079, f"Expected 5,819,079 rows, got {total_cnt}"

        def save_table(name, df_table):
            out_path = f"{analytics_base}/{name}"
            df_table.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(out_path)
            c = df_table.count()
            logger.info(f"Saved {name} ({c} rows) to {out_path}")
            return c

        table_counts = {}

        # ----------------------------------------------------------------------
        # 1. kpi_overall (1 row)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_overall...")
        df_overall = add_derived_metrics(df.groupBy().agg(*get_base_metrics()))
        table_counts["kpi_overall"] = save_table("kpi_overall", df_overall)

        # ----------------------------------------------------------------------
        # 2. kpi_by_month (12 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_month...")
        df_month = add_derived_metrics(
            df.groupBy("MONTH", "MONTH_NAME").agg(*get_base_metrics())
        ).orderBy("MONTH")
        table_counts["kpi_by_month"] = save_table("kpi_by_month", df_month)

        # ----------------------------------------------------------------------
        # 3. kpi_by_day_of_week (7 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_day_of_week...")
        df_dow = add_derived_metrics(
            df.groupBy("DAY_OF_WEEK", "DAY_OF_WEEK_NAME").agg(*get_base_metrics())
        ).orderBy("DAY_OF_WEEK")
        table_counts["kpi_by_day_of_week"] = save_table("kpi_by_day_of_week", df_dow)

        # ----------------------------------------------------------------------
        # 4. kpi_by_dep_hour (24 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_dep_hour...")
        df_hour = add_derived_metrics(
            df.groupBy("SCHED_DEP_HOUR").agg(*get_base_metrics())
        ).orderBy("SCHED_DEP_HOUR")
        table_counts["kpi_by_dep_hour"] = save_table("kpi_by_dep_hour", df_hour)

        # ----------------------------------------------------------------------
        # 5. kpi_by_dow_hour (168 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_dow_hour...")
        df_dow_hour = add_derived_metrics(
            df.groupBy("DAY_OF_WEEK", "DAY_OF_WEEK_NAME", "SCHED_DEP_HOUR").agg(*get_base_metrics())
        ).orderBy("DAY_OF_WEEK", "SCHED_DEP_HOUR")
        table_counts["kpi_by_dow_hour"] = save_table("kpi_by_dow_hour", df_dow_hour)

        # ----------------------------------------------------------------------
        # 6. kpi_by_airline (14 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_airline...")
        df_airline = add_derived_metrics(
            df.groupBy("AIRLINE", "AIRLINE_NAME").agg(*get_base_metrics())
        ).orderBy(F.desc("total_flights"))
        table_counts["kpi_by_airline"] = save_table("kpi_by_airline", df_airline)

        # ----------------------------------------------------------------------
        # 7. kpi_by_airline_month (168 rows: complete 14 airlines x 12 months grid)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_airline_month...")
        df_airlines_distinct = df.select("AIRLINE", "AIRLINE_NAME").distinct()
        df_months_distinct = df.select("MONTH", "MONTH_NAME").distinct()
        df_grid = df_airlines_distinct.crossJoin(df_months_distinct)

        df_am_agg = df.groupBy("AIRLINE", "MONTH").agg(*get_base_metrics())
        df_am_joined = df_grid.join(df_am_agg, on=["AIRLINE", "MONTH"], how="left")
        
        num_fill_cols = [
            "total_flights", "flown_flights", "cancelled_flights", "diverted_flights",
            "delayed_arr_15", "delayed_dep_15", "sum_arr_delay_min", "sum_dep_delay_min",
            "sum_delay_minutes_pos", "sum_cause_air_system", "sum_cause_security",
            "sum_cause_airline", "sum_cause_late_aircraft", "sum_cause_weather",
            "sum_arr_delay_when_delayed_min"
        ]
        fill_dict = {c: 0 for c in num_fill_cols}
        df_am_filled = df_am_joined.na.fill(fill_dict)

        df_airline_month = add_derived_metrics(df_am_filled).orderBy("AIRLINE", "MONTH")
        table_counts["kpi_by_airline_month"] = save_table("kpi_by_airline_month", df_airline_month)

        # ----------------------------------------------------------------------
        # 8. kpi_by_origin_airport (322 + 1 UNMAPPED = 323 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_origin_airport...")
        w_origin = Window.partitionBy(F.when(F.col("meets_min_volume") == 1, F.lit(1)).otherwise(F.lit(0))) \
            .orderBy(F.asc("arr_delay_rate_15"), F.desc("flown_flights"), F.asc("ORIGIN_AIRPORT"))

        df_orig_agg = df.groupBy(
            F.col("ORIGIN_AIRPORT_KEY").alias("ORIGIN_AIRPORT"),
            F.col("ORIGIN_NAME_KEY").alias("ORIGIN_AIRPORT_NAME"),
            F.col("ORIGIN_CITY_KEY").alias("ORIGIN_CITY"),
            F.col("ORIGIN_STATE_KEY").alias("ORIGIN_STATE")
        ).agg(
            F.first("ORIGIN_LAT", ignorenulls=True).alias("ORIGIN_LAT"),
            F.first("ORIGIN_LON", ignorenulls=True).alias("ORIGIN_LON"),
            *get_base_metrics()
        )
        
        df_orig_kpi = add_derived_metrics(df_orig_agg) \
            .withColumn("HAS_COORDS", F.when(F.col("ORIGIN_LAT").isNotNull() & F.col("ORIGIN_LON").isNotNull(), 1).otherwise(0)) \
            .withColumn("meets_min_volume", F.when(F.col("flown_flights") >= min_flown_airport, 1).otherwise(0)) \
            .withColumn("rank_on_time", F.when(F.col("meets_min_volume") == 1, F.dense_rank().over(w_origin))) \
            .orderBy(F.desc("total_flights"))
        table_counts["kpi_by_origin_airport"] = save_table("kpi_by_origin_airport", df_orig_kpi)

        # ----------------------------------------------------------------------
        # 9. kpi_by_dest_airport (322 + 1 UNMAPPED = 323 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_dest_airport...")
        w_dest = Window.partitionBy(F.when(F.col("meets_min_volume") == 1, F.lit(1)).otherwise(F.lit(0))) \
            .orderBy(F.asc("arr_delay_rate_15"), F.desc("flown_flights"), F.asc("DESTINATION_AIRPORT"))

        df_dest_agg = df.groupBy(
            F.col("DEST_AIRPORT_KEY").alias("DESTINATION_AIRPORT"),
            F.col("DEST_NAME_KEY").alias("DEST_AIRPORT_NAME"),
            F.col("DEST_CITY_KEY").alias("DEST_CITY"),
            F.col("DEST_STATE_KEY").alias("DEST_STATE")
        ).agg(
            F.first("DEST_LAT", ignorenulls=True).alias("DEST_LAT"),
            F.first("DEST_LON", ignorenulls=True).alias("DEST_LON"),
            *get_base_metrics()
        )
        
        df_dest_kpi = add_derived_metrics(df_dest_agg) \
            .withColumn("HAS_COORDS", F.when(F.col("DEST_LAT").isNotNull() & F.col("DEST_LON").isNotNull(), 1).otherwise(0)) \
            .withColumn("meets_min_volume", F.when(F.col("flown_flights") >= min_flown_airport, 1).otherwise(0)) \
            .withColumn("rank_on_time", F.when(F.col("meets_min_volume") == 1, F.dense_rank().over(w_dest))) \
            .orderBy(F.desc("total_flights"))
        table_counts["kpi_by_dest_airport"] = save_table("kpi_by_dest_airport", df_dest_kpi)

        # ----------------------------------------------------------------------
        # 10. kpi_by_route (ROUTE non-null with UNMAPPED bucket for reconciliation)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_route...")
        w_route = Window.partitionBy(F.when(F.col("meets_min_volume") == 1, F.lit(1)).otherwise(F.lit(0))) \
            .orderBy(F.asc("arr_delay_rate_15"), F.desc("flown_flights"), F.asc("ROUTE"))

        df_route_agg = df.groupBy(
            F.col("ROUTE_KEY").alias("ROUTE"),
            F.when(F.col("ROUTE_KEY") == "UNMAPPED", F.lit("UNMAPPED")).otherwise(F.col("ORIGIN_AIRPORT_KEY")).alias("ORIGIN_AIRPORT"),
            F.when(F.col("ROUTE_KEY") == "UNMAPPED", F.lit("UNMAPPED")).otherwise(F.col("DEST_AIRPORT_KEY")).alias("DESTINATION_AIRPORT")
        ).agg(
            F.round(F.avg("DISTANCE"), 1).alias("avg_distance"),
            *get_base_metrics()
        )

        df_route_kpi = add_derived_metrics(df_route_agg) \
            .withColumn("meets_min_volume", F.when((F.col("ROUTE") != "UNMAPPED") & (F.col("flown_flights") >= min_flown_route), 1).otherwise(0)) \
            .withColumn("rank_on_time", F.when(F.col("meets_min_volume") == 1, F.dense_rank().over(w_route))) \
            .orderBy(F.desc("total_flights"))
        table_counts["kpi_by_route"] = save_table("kpi_by_route", df_route_kpi)

        # ----------------------------------------------------------------------
        # 11. kpi_by_origin_state
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_origin_state...")
        df_state = add_derived_metrics(
            df.groupBy(F.col("ORIGIN_STATE_KEY").alias("ORIGIN_STATE")).agg(*get_base_metrics())
        ).orderBy(F.desc("total_flights"))
        table_counts["kpi_by_origin_state"] = save_table("kpi_by_origin_state", df_state)

        # ----------------------------------------------------------------------
        # 12. kpi_by_date (365 rows)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_date...")
        df_date = add_derived_metrics(
            df.groupBy(
                "FLIGHT_DATE", "MONTH", "DAY", "DAY_OF_WEEK_NAME", "SEASON", "IS_WEEKEND", "IS_HOLIDAY_WINDOW"
            ).agg(*get_base_metrics())
        ).orderBy("FLIGHT_DATE")
        table_counts["kpi_by_date"] = save_table("kpi_by_date", df_date)

        # ----------------------------------------------------------------------
        # 13. kpi_by_segment (Long format: segment_type, segment_value)
        # ----------------------------------------------------------------------
        logger.info("Building kpi_by_segment (long format)...")
        segments = ["DISTANCE_BAND", "SEASON", "DEP_TIME_BLOCK", "IS_HOLIDAY_WINDOW", "IS_WEEKEND"]
        segment_dfs = []
        for seg in segments:
            seg_df = df.groupBy(F.lit(seg).alias("segment_type"), F.col(seg).cast("string").alias("segment_value")) \
                .agg(*get_base_metrics())
            segment_dfs.append(add_derived_metrics(seg_df))

        df_segment = segment_dfs[0]
        for next_df in segment_dfs[1:]:
            df_segment = df_segment.unionByName(next_df)
        table_counts["kpi_by_segment"] = save_table("kpi_by_segment", df_segment)

        # ----------------------------------------------------------------------
        # 14. cancellations_by_reason_month
        # ----------------------------------------------------------------------
        logger.info("Building cancellations_by_reason_month...")
        df_canc = df.filter(F.col("CANCELLED") == 1)
        w_canc_m = Window.partitionBy("MONTH")
        df_canc_m = df_canc.groupBy("MONTH", "MONTH_NAME", "CANCELLATION_REASON", "CANCELLATION_REASON_DESC") \
            .agg(F.count("*").alias("cancelled_flights")) \
            .withColumn("month_total_cancelled", F.sum("cancelled_flights").over(w_canc_m)) \
            .withColumn("cancel_share_of_month", F.round(F.col("cancelled_flights") / F.col("month_total_cancelled"), 6)) \
            .drop("month_total_cancelled") \
            .orderBy("MONTH", F.desc("cancelled_flights"))
        table_counts["cancellations_by_reason_month"] = save_table("cancellations_by_reason_month", df_canc_m)

        # ----------------------------------------------------------------------
        # 15. cancellations_by_reason_airline
        # ----------------------------------------------------------------------
        logger.info("Building cancellations_by_reason_airline...")
        w_canc_a = Window.partitionBy("AIRLINE")
        df_canc_a = df_canc.groupBy("AIRLINE", "AIRLINE_NAME", "CANCELLATION_REASON", "CANCELLATION_REASON_DESC") \
            .agg(F.count("*").alias("cancelled_flights")) \
            .withColumn("airline_total_cancelled", F.sum("cancelled_flights").over(w_canc_a)) \
            .withColumn("cancel_share_of_airline", F.round(F.col("cancelled_flights") / F.col("airline_total_cancelled"), 6)) \
            .drop("airline_total_cancelled") \
            .orderBy("AIRLINE", F.desc("cancelled_flights"))
        table_counts["cancellations_by_reason_airline"] = save_table("cancellations_by_reason_airline", df_canc_a)

        # ----------------------------------------------------------------------
        # 16. causes_by_month
        # ----------------------------------------------------------------------
        logger.info("Building causes_by_month...")
        df_causes_m = df.filter(F.col("HAS_CAUSE_DATA") == 1).groupBy("MONTH", "MONTH_NAME").agg(
            F.count("*").alias("total_delayed_15"),
            F.sum("AIR_SYSTEM_DELAY").alias("sum_cause_air_system"),
            F.sum("SECURITY_DELAY").alias("sum_cause_security"),
            F.sum("AIRLINE_DELAY").alias("sum_cause_airline"),
            F.sum("LATE_AIRCRAFT_DELAY").alias("sum_cause_late_aircraft"),
            F.sum("WEATHER_DELAY").alias("sum_cause_weather")
        ).withColumn(
            "total_cause_min",
            F.col("sum_cause_air_system") + F.col("sum_cause_security") + F.col("sum_cause_airline") +
            F.col("sum_cause_late_aircraft") + F.col("sum_cause_weather")
        ).withColumn("share_cause_air_system", F.round(F.col("sum_cause_air_system") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_security", F.round(F.col("sum_cause_security") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_airline", F.round(F.col("sum_cause_airline") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_late_aircraft", F.round(F.col("sum_cause_late_aircraft") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_weather", F.round(F.col("sum_cause_weather") / F.col("total_cause_min"), 6)) \
         .orderBy("MONTH")
        table_counts["causes_by_month"] = save_table("causes_by_month", df_causes_m)

        # ----------------------------------------------------------------------
        # 17. causes_by_airline
        # ----------------------------------------------------------------------
        logger.info("Building causes_by_airline...")
        df_causes_a = df.filter(F.col("HAS_CAUSE_DATA") == 1).groupBy("AIRLINE", "AIRLINE_NAME").agg(
            F.count("*").alias("total_delayed_15"),
            F.sum("AIR_SYSTEM_DELAY").alias("sum_cause_air_system"),
            F.sum("SECURITY_DELAY").alias("sum_cause_security"),
            F.sum("AIRLINE_DELAY").alias("sum_cause_airline"),
            F.sum("LATE_AIRCRAFT_DELAY").alias("sum_cause_late_aircraft"),
            F.sum("WEATHER_DELAY").alias("sum_cause_weather")
        ).withColumn(
            "total_cause_min",
            F.col("sum_cause_air_system") + F.col("sum_cause_security") + F.col("sum_cause_airline") +
            F.col("sum_cause_late_aircraft") + F.col("sum_cause_weather")
        ).withColumn("share_cause_air_system", F.round(F.col("sum_cause_air_system") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_security", F.round(F.col("sum_cause_security") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_airline", F.round(F.col("sum_cause_airline") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_late_aircraft", F.round(F.col("sum_cause_late_aircraft") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_weather", F.round(F.col("sum_cause_weather") / F.col("total_cause_min"), 6)) \
         .orderBy(F.desc("total_delayed_15"))
        table_counts["causes_by_airline"] = save_table("causes_by_airline", df_causes_a)

        # ----------------------------------------------------------------------
        # 18. causes_by_origin_airport
        # ----------------------------------------------------------------------
        logger.info("Building causes_by_origin_airport...")
        df_causes_orig = df.filter(F.col("HAS_CAUSE_DATA") == 1).groupBy(
            F.col("ORIGIN_AIRPORT_KEY").alias("ORIGIN_AIRPORT"),
            F.col("ORIGIN_NAME_KEY").alias("ORIGIN_AIRPORT_NAME")
        ).agg(
            F.count("*").alias("total_delayed_15"),
            F.sum("AIR_SYSTEM_DELAY").alias("sum_cause_air_system"),
            F.sum("SECURITY_DELAY").alias("sum_cause_security"),
            F.sum("AIRLINE_DELAY").alias("sum_cause_airline"),
            F.sum("LATE_AIRCRAFT_DELAY").alias("sum_cause_late_aircraft"),
            F.sum("WEATHER_DELAY").alias("sum_cause_weather")
        ).withColumn(
            "total_cause_min",
            F.col("sum_cause_air_system") + F.col("sum_cause_security") + F.col("sum_cause_airline") +
            F.col("sum_cause_late_aircraft") + F.col("sum_cause_weather")
        ).withColumn("share_cause_air_system", F.round(F.col("sum_cause_air_system") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_security", F.round(F.col("sum_cause_security") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_airline", F.round(F.col("sum_cause_airline") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_late_aircraft", F.round(F.col("sum_cause_late_aircraft") / F.col("total_cause_min"), 6)) \
         .withColumn("share_cause_weather", F.round(F.col("sum_cause_weather") / F.col("total_cause_min"), 6)) \
         .orderBy(F.desc("total_delayed_15"))
        table_counts["causes_by_origin_airport"] = save_table("causes_by_origin_airport", df_causes_orig)

        # ----------------------------------------------------------------------
        # 19. delay_category_by_airline
        # ----------------------------------------------------------------------
        logger.info("Building delay_category_by_airline...")
        df_del_cat = df.groupBy("AIRLINE", "AIRLINE_NAME").agg(
            F.count("*").alias("total_flights"),
            F.sum(F.when(F.col("DELAY_CATEGORY") == "On Time/Early", 1).otherwise(0)).alias("cat_on_time_early"),
            F.sum(F.when(F.col("DELAY_CATEGORY") == "Minor", 1).otherwise(0)).alias("cat_minor"),
            F.sum(F.when(F.col("DELAY_CATEGORY") == "Moderate", 1).otherwise(0)).alias("cat_moderate"),
            F.sum(F.when(F.col("DELAY_CATEGORY") == "Major", 1).otherwise(0)).alias("cat_major"),
            F.sum(F.when(F.col("DELAY_CATEGORY") == "Severe", 1).otherwise(0)).alias("cat_severe"),
            F.sum(F.when(F.col("DELAY_CATEGORY").isNull(), 1).otherwise(0)).alias("cat_non_flown")
        ).orderBy("AIRLINE")
        table_counts["delay_category_by_airline"] = save_table("delay_category_by_airline", df_del_cat)

        total_runtime = time.perf_counter() - start_time
        metrics = {
            "script": "build_analytics.py",
            "runtime_sec": round(total_runtime, 2),
            "tables_generated": len(table_counts),
            "table_row_counts": table_counts,
            "status": "SUCCESS"
        }
        metrics_path = PROJECT_ROOT / "logs" / "metrics" / "build_analytics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w", encoding="utf-8") as mf:
            json.dump(metrics, mf, indent=2)
        logger.info(f"Saved analytics metrics ({total_runtime:.2f}s) to {metrics_path}")
        logger.info(f"[SUCCESS] All 19 analytics tables generated in {total_runtime:.2f}s!")
        return metrics

    finally:
        df.unpersist()
        spark.stop()


if __name__ == "__main__":
    build_analytics_pipeline()
