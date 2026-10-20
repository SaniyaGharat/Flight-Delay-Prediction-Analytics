#!/usr/bin/env python3
"""
src/processing/build_dim_date.py - Phase 4 Date Dimension Builder.

Generates a standard 365-day calendar dimension table for the year 2015:
- DATE, YEAR, MONTH, MONTH_NAME, DAY, DAY_OF_WEEK, DAY_OF_WEEK_NAME,
  WEEK_OF_YEAR, IS_WEEKEND, IS_HOLIDAY_WINDOW, SEASON.
Writes output to HDFS processed/dim_date (Parquet, snappy).
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
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
)


def setup_logger(project_root: Path) -> logging.Logger:
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "enrichment.log"

    logger = logging.getLogger("Phase4-DimDate")
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


def compute_holiday_window_dates(holiday_strings: List[str], window_days: int = 3) -> Set[date]:
    """Computes all dates within +/- window_days of the configured holidays."""
    holiday_dates = [date.fromisoformat(h) for h in holiday_strings]
    window_set: Set[date] = set()
    for h in holiday_dates:
        for offset in range(-window_days, window_days + 1):
            window_set.add(h + timedelta(days=offset))
    return window_set


def build_dim_date_pipeline():
    logger = setup_logger(PROJECT_ROOT)
    logger.info("============================================================")
    logger.info("Starting Phase 4 Date Dimension Generation (build_dim_date)...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)
    dim_date_path = f"{namenode_uri}{processed_dir}/dim_date"

    holidays_cfg = config.get("holidays_2015", [
        "2015-01-01", "2015-01-19", "2015-02-16", "2015-05-25",
        "2015-07-03", "2015-09-07", "2015-10-12", "2015-11-11",
        "2015-11-26", "2015-12-25"
    ])
    logger.info(f"Loaded {len(holidays_cfg)} federal holidays for 2015: {holidays_cfg}")
    holiday_window_dates = compute_holiday_window_dates(holidays_cfg, window_days=3)
    logger.info(f"Total days falling into +/-3 day holiday window: {len(holiday_window_dates)}")

    spark = get_spark_session(
        app_name="FlightDelay-Phase4-DimDate",
        enable_hdfs=True,
    )

    try:
        month_names = {
            1: "January", 2: "February", 3: "March", 4: "April",
            5: "May", 6: "June", 7: "July", 8: "August",
            9: "September", 10: "October", 11: "November", 12: "December"
        }
        day_names = {
            1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
            5: "Friday", 6: "Saturday", 7: "Sunday"
        }

        start_date = date(2015, 1, 1)
        end_date = date(2015, 12, 31)
        curr = start_date

        rows = []
        while curr <= end_date:
            y = curr.year
            m = curr.month
            d = curr.day
            iso_dow = curr.isoweekday()  # 1=Mon .. 7=Sun
            is_weekend = 1 if iso_dow in (6, 7) else 0
            is_holiday_win = 1 if curr in holiday_window_dates else 0
            week_of_year = curr.isocalendar()[1]

            if m in (12, 1, 2):
                season = "Winter"
            elif m in (3, 4, 5):
                season = "Spring"
            elif m in (6, 7, 8):
                season = "Summer"
            else:
                season = "Fall"

            rows.append((
                curr,
                y,
                m,
                month_names[m],
                d,
                iso_dow,
                day_names[iso_dow],
                int(week_of_year),
                is_weekend,
                is_holiday_win,
                season
            ))
            curr += timedelta(days=1)

        schema = StructType([
            StructField("DATE", DateType(), False),
            StructField("YEAR", IntegerType(), False),
            StructField("MONTH", IntegerType(), False),
            StructField("MONTH_NAME", StringType(), False),
            StructField("DAY", IntegerType(), False),
            StructField("DAY_OF_WEEK", IntegerType(), False),
            StructField("DAY_OF_WEEK_NAME", StringType(), False),
            StructField("WEEK_OF_YEAR", IntegerType(), False),
            StructField("IS_WEEKEND", IntegerType(), False),
            StructField("IS_HOLIDAY_WINDOW", IntegerType(), False),
            StructField("SEASON", StringType(), False),
        ])

        df_dim_date = spark.createDataFrame(rows, schema=schema)
        cnt = df_dim_date.count()
        logger.info(f"Generated dim_date DataFrame with {cnt} rows.")
        assert cnt == 365, f"Expected 365 rows in 2015 dim_date, got {cnt}"

        logger.info(f"Writing dim_date to {dim_date_path} (Parquet, snappy, 1 file)...")
        df_dim_date.coalesce(1).write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .parquet(dim_date_path)

        logger.info("[SUCCESS] dim_date successfully written to HDFS.")

    finally:
        spark.stop()


if __name__ == "__main__":
    build_dim_date_pipeline()
