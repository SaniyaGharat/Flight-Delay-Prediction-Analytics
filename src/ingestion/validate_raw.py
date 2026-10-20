#!/usr/bin/env python3
"""
validate_raw.py - Validate raw CSV datasets read directly from HDFS using PySpark.
Ensures header=True, inferSchema=False (all string columns), displays schema and sample rows,
and validates that HDFS row counts equal (wc -l - 1) of the local raw CSVs.
"""

import sys
import os
from pathlib import Path
from typing import Dict, Any, Tuple, List

try:
    import yaml
except ImportError:
    yaml = None

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session


def load_config() -> Dict[str, Any]:
    """Loads configuration settings from config/config.yaml."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_local_line_count(file_path: Path) -> int:
    """Computes total line count (equivalent to wc -l) of a local text file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Local file does not exist: {file_path}")
    count = 0
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            count += chunk.count(b"\n")
    return count


def validate_raw_datasets() -> None:
    """Validates raw CSV files stored in HDFS against local source files."""
    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    hdfs_paths = hdfs_cfg.get("paths", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    raw_cfg = config.get("raw_data", {})
    local_dir = PROJECT_ROOT / raw_cfg.get("local_dir", "data/raw")
    file_names = raw_cfg.get("files", {})

    datasets: List[Tuple[str, Path, str]] = [
        (
            "airlines",
            local_dir / file_names.get("airlines", "airlines.csv"),
            f"{namenode_uri}{hdfs_paths.get('airlines', '/user/saniya/flight_delay/raw/airlines/airlines.csv')}",
        ),
        (
            "airports",
            local_dir / file_names.get("airports", "airports.csv"),
            f"{namenode_uri}{hdfs_paths.get('airports', '/user/saniya/flight_delay/raw/airports/airports.csv')}",
        ),
        (
            "flights",
            local_dir / file_names.get("flights", "flights.csv"),
            f"{namenode_uri}{hdfs_paths.get('flights', '/user/saniya/flight_delay/raw/flights/flights.csv')}",
        ),
    ]

    print("\n" + "=" * 80)
    print("PHASE 1: PYSPARK RAW HDFS DATASET VALIDATION")
    print("=" * 80)

    spark = get_spark_session(app_name="FlightDelay-ValidateRawHDFS", enable_hdfs=True)

    try:
        for name, local_path, hdfs_uri in datasets:
            print("\n" + "-" * 80)
            print(f"DATASET: {name.upper()}")
            print(f"HDFS Location: {hdfs_uri}")
            print(f"Local Source:  {local_path}")
            print("-" * 80)

            # Compute local row count expectation: (wc -l) - 1 header line
            local_lines = get_local_line_count(local_path)
            expected_data_rows = local_lines - 1
            print(f"Local file stats: {local_lines:,} total lines (expected data rows: {expected_data_rows:,})")

            # Read CSV directly from HDFS with PySpark (no schema inference, header enabled)
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(hdfs_uri)
            )

            hdfs_rows = df.count()
            hdfs_cols = len(df.columns)

            print(f"\nHDFS PySpark Row Count:    {hdfs_rows:,}")
            print(f"HDFS PySpark Column Count: {hdfs_cols}")
            print(f"\n--- Schema for {name} (All String Types, InferSchema=False) ---")
            df.printSchema()

            print(f"\n--- Sample 5 Records for {name} ---")
            df.show(5, truncate=False)

            # Verification of row count
            if hdfs_rows != expected_data_rows:
                raise ValueError(
                    f"[ERROR] Row count mismatch for {name}! "
                    f"HDFS PySpark count ({hdfs_rows:,}) != Expected ({expected_data_rows:,})"
                )

            print(f"[VERIFIED] {name}: HDFS row count ({hdfs_rows:,}) matches local (wc -l - 1) exactly!")

        print("\n" + "=" * 80)
        print("[SUCCESS] All raw datasets in HDFS successfully validated with PySpark!")
        print("=" * 80 + "\n")

    finally:
        spark.stop()


def main() -> None:
    try:
        validate_raw_datasets()
    except Exception as e:
        print(f"\n[FATAL ERROR during validation] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
