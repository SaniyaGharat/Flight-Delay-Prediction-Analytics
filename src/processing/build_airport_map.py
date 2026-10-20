#!/usr/bin/env python3
"""
src/processing/build_airport_map.py - Phase 3 Airport ID Mapping Pipeline.

Discovers the mapping from 5-digit DOT numeric airport IDs (used exclusively in October 2015)
back to standard 3-letter alphabetic IATA airport codes by joining repeating flight schedules
across non-October months.
"""

import os
import sys
import time
import logging
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


def setup_logger(project_root: Path) -> logging.Logger:
    """Configures logging to console and logs/cleaning.log."""
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "cleaning.log"

    logger = logging.getLogger("Phase3-AirportMapping")
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


def build_airport_id_mapping() -> Dict[str, Any]:
    """Builds and validates the DOT numeric airport ID to IATA code mapping."""
    start_time = time.time()
    logger = setup_logger(PROJECT_ROOT)
    logger.info("============================================================")
    logger.info("Starting Phase 3 Airport ID Mapping Discovery Pipeline...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    raw_dir = hdfs_cfg.get("raw_dir", f"/user/{user}/flight_delay/raw").replace("${USER}", user)
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)

    flights_hdfs_path = f"{namenode_uri}{raw_dir}/flights/flights.csv"
    airports_hdfs_path = f"{namenode_uri}{raw_dir}/airports/airports.csv"
    map_parquet_path = f"{namenode_uri}{processed_dir}/airport_id_map"
    map_csv_path = PROJECT_ROOT / "output" / "cleaning" / "airport_id_map.csv"
    map_csv_path.parent.mkdir(parents=True, exist_ok=True)

    extra_spark_cfg = {
        "spark.driver.memory": "3g",
        "spark.sql.shuffle.partitions": "16",
    }
    spark = get_spark_session(
        app_name="FlightDelay-Phase3-AirportMapping",
        enable_hdfs=True,
        extra_configs=extra_spark_cfg,
    )

    try:
        logger.info(f"Reading raw flights dataset from {flights_hdfs_path}...")
        df_flights = spark.read.csv(flights_hdfs_path, header=True, inferSchema=False)
        df_airports = spark.read.csv(airports_hdfs_path, header=True, inferSchema=False)

        airports_iata_set = set(
            r["IATA_CODE"]
            for r in df_airports.select("IATA_CODE").distinct().collect()
            if r["IATA_CODE"]
        )
        logger.info(f"Loaded {len(airports_iata_set)} reference airport IATA codes from airports.csv.")

        join_keys = ["AIRLINE", "FLIGHT_NUMBER", "SCHEDULED_DEPARTURE", "SCHEDULED_ARRIVAL", "DISTANCE"]

        logger.info("Partitioning flights into October (numeric IDs) and Non-October (IATA codes)...")
        df_oct = df_flights.filter(F.col("MONTH") == "10")
        df_nonoct = df_flights.filter(
            (F.col("MONTH") != "10")
            & F.col("ORIGIN_AIRPORT").rlike("^[A-Z]{3}$")
            & F.col("DESTINATION_AIRPORT").rlike("^[A-Z]{3}$")
        )

        oct_flights_count = df_oct.count()
        logger.info(f"Total October flights: {oct_flights_count:,}")

        # Aggregate schedules to avoid explosive cartesian product
        oct_sched = df_oct.groupBy(*(join_keys + ["ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])).agg(
            F.count("*").alias("oct_cnt")
        )
        nonoct_sched = df_nonoct.groupBy(*(join_keys + ["ORIGIN_AIRPORT", "DESTINATION_AIRPORT"])).agg(
            F.count("*").alias("nonoct_cnt")
        )

        logger.info("Joining October and Non-October schedules on key (AIRLINE, FLIGHT_NUM, SCHED_DEP, SCHED_ARR, DISTANCE)...")
        joined = oct_sched.alias("oct").join(
            nonoct_sched.alias("non"),
            on=join_keys,
            how="inner",
        )

        # Build candidate pairs from ORIGIN and DESTINATION separately
        orig_pairs = joined.select(
            F.col("oct.ORIGIN_AIRPORT").alias("airport_id"),
            F.col("non.ORIGIN_AIRPORT").alias("iata_code"),
            F.col("oct.oct_cnt").alias("votes"),
        )
        dest_pairs = joined.select(
            F.col("oct.DESTINATION_AIRPORT").alias("airport_id"),
            F.col("non.DESTINATION_AIRPORT").alias("iata_code"),
            F.col("oct.oct_cnt").alias("votes"),
        )

        all_pairs = orig_pairs.unionByName(dest_pairs)

        # Filter strictly numeric DOT IDs
        numeric_pairs = all_pairs.filter(F.col("airport_id").rlike(r"^\d+$"))

        # Aggregate votes per (airport_id, iata_code)
        pair_counts = numeric_pairs.groupBy("airport_id", "iata_code").agg(
            F.sum("votes").alias("support")
        )

        # Window to find dominant IATA per numeric ID
        w_id = Window.partitionBy("airport_id")
        w_id_ord = Window.partitionBy("airport_id").orderBy(F.desc("support"))

        ranked_pairs = pair_counts.withColumn("total_votes", F.sum("support").over(w_id)) \
            .withColumn("vote_pct", F.col("support") / F.col("total_votes")) \
            .withColumn("rank", F.row_number().over(w_id_ord))

        winners = ranked_pairs.filter(F.col("rank") == 1)

        # Find distinct numeric IDs present in October
        oct_origin_ids = set([r[0] for r in df_oct.select("ORIGIN_AIRPORT").distinct().collect()])
        oct_dest_ids = set([r[0] for r in df_oct.select("DESTINATION_AIRPORT").distinct().collect()])
        all_oct_ids = set([x for x in (oct_origin_ids | oct_dest_ids) if x and x.isdigit()])
        total_numeric_ids = len(all_oct_ids)

        # Threshold rules: support >= 3 and winner has >= 90% of votes
        accepted_df = winners.filter((F.col("support") >= 3) & (F.col("vote_pct") >= 0.90))
        ambiguous_df = winners.filter((F.col("support") >= 3) & (F.col("vote_pct") < 0.90))
        low_support_df = winners.filter(F.col("support") < 3)

        accepted_rows = accepted_df.collect()
        ambiguous_rows = ambiguous_df.collect()
        low_support_rows = low_support_df.collect()

        mapped_dict = {r["airport_id"]: r["iata_code"] for r in accepted_rows}
        mapped_ids = set(mapped_dict.keys())
        ambiguous_ids = set([r["airport_id"] for r in ambiguous_rows])
        unmapped_ids = all_oct_ids - mapped_ids

        logger.info(f"Distinct numeric DOT IDs found in October: {total_numeric_ids}")
        logger.info(f"Successfully mapped IDs (support >= 3, >= 90%): {len(mapped_ids)}")
        logger.info(f"Ambiguous IDs (support >= 3, < 90%): {len(ambiguous_ids)}")
        logger.info(f"Unmapped IDs total: {len(unmapped_ids)} ({sorted(list(unmapped_ids))})")

        # Count October rows affected by unmapped IDs
        unmapped_affected_rows = df_oct.filter(
            F.col("ORIGIN_AIRPORT").isin(list(unmapped_ids)) | F.col("DESTINATION_AIRPORT").isin(list(unmapped_ids))
        ).count()
        unmapped_pct = (unmapped_affected_rows / oct_flights_count) * 100.0
        logger.info(f"October flight rows affected by unmapped IDs: {unmapped_affected_rows:,} ({unmapped_pct:.2f}%)")

        # Sanity Check 1: One-to-one check (multiple IDs -> same IATA)
        iata_to_ids: Dict[str, List[str]] = {}
        for num_id, iata in mapped_dict.items():
            iata_to_ids.setdefault(iata, []).append(num_id)
        
        collisions = {iata: ids for iata, ids in iata_to_ids.items() if len(ids) > 1}
        if collisions:
            logger.warn(f"[CHECK 1] Found {len(collisions)} IATA codes mapped to multiple numeric IDs:")
            for iata, ids in sorted(collisions.items()):
                logger.warn(f"  IATA '{iata}' <- IDs {ids}")
        else:
            logger.info("[CHECK 1] PASSED: Mapping is strictly one-to-one (0 collisions).")

        # Sanity Check 2: Every mapped IATA exists in airports.csv
        invalid_iatas = set(mapped_dict.values()) - airports_iata_set
        if invalid_iatas:
            logger.error(f"[CHECK 2] FAILED: {len(invalid_iatas)} mapped IATA codes missing from airports.csv: {invalid_iatas}")
        else:
            logger.info(f"[CHECK 2] PASSED: All {len(mapped_dict)} mapped IATA codes exist in airports.csv.")

        # Sanity Check 3: Anchor spot-checks
        anchors = {
            "10397": "ATL",
            "13930": "ORD",
            "11298": "DFW",
            "12478": "JFK",
            "12892": "LAX",
            "11618": "EWR",
            "14771": "SFO",
        }
        anchor_results = []
        logger.info("=== Anchor Spot-Checks (DOT Scheme Anchors) ===")
        for aid, expected_iata in anchors.items():
            actual_iata = mapped_dict.get(aid)
            is_match = (actual_iata == expected_iata)
            status = "MATCH" if is_match else "MISMATCH"
            # Get support stats
            r_match = [r for r in accepted_rows if r["airport_id"] == aid]
            sup = r_match[0]["support"] if r_match else 0
            pct = r_match[0]["vote_pct"] if r_match else 0.0
            logger.info(f"  Anchor {aid}: Expected={expected_iata}, Got={actual_iata} (support={sup:,}, {pct:.2%}) -> {status}")
            anchor_results.append({
                "airport_id": aid,
                "expected": expected_iata,
                "actual": actual_iata,
                "support": sup,
                "vote_pct": pct,
                "status": status,
            })

        # Save to HDFS as Parquet
        logger.info(f"Saving mapping Parquet to HDFS: {map_parquet_path}...")
        save_df = accepted_df.select(
            F.col("airport_id"),
            F.col("iata_code"),
            F.col("support"),
            F.round(F.col("vote_pct") * 100, 2).alias("vote_pct")
        ).coalesce(1)

        save_df.write.mode("overwrite").parquet(map_parquet_path)
        logger.info(f"[SUCCESS] Saved airport mapping Parquet to {map_parquet_path}")

        # Save to local CSV for auditing
        logger.info(f"Writing local audit CSV to {map_csv_path}...")
        with open(map_csv_path, "w", encoding="utf-8") as f:
            f.write("airport_id,iata_code,support,vote_pct\n")
            for r in sorted(accepted_rows, key=lambda x: int(x["airport_id"])):
                f.write(f"{r['airport_id']},{r['iata_code']},{r['support']},{r['vote_pct'] * 100:.2f}\n")
        logger.info(f"[SUCCESS] Saved {len(accepted_rows)} records to {map_csv_path}")

        elapsed_sec = time.time() - start_time
        logger.info(f"Airport ID mapping pipeline finished successfully in {elapsed_sec:.2f}s.")

        return {
            "total_numeric_ids": total_numeric_ids,
            "mapped_count": len(mapped_ids),
            "ambiguous_count": len(ambiguous_ids),
            "ambiguous_ids": sorted(list(ambiguous_ids)),
            "unmapped_count": len(unmapped_ids),
            "unmapped_ids": sorted(list(unmapped_ids)),
            "unmapped_affected_rows": unmapped_affected_rows,
            "unmapped_pct": unmapped_pct,
            "anchor_results": anchor_results,
            "collisions": collisions,
            "invalid_iatas": list(invalid_iatas),
            "runtime_sec": elapsed_sec,
        }

    finally:
        spark.stop()
        logger.info("SparkSession stopped.")


def main():
    results = build_airport_id_mapping()
    print("\n=== AIRPORT ID MAPPING SUMMARY ===")
    print(f"Total Numeric IDs: {results['total_numeric_ids']}")
    print(f"Mapped IDs: {results['mapped_count']}")
    print(f"Ambiguous IDs: {results['ambiguous_count']}")
    print(f"Unmapped IDs: {results['unmapped_count']} ({results['unmapped_ids']})")
    print(f"October Rows Affected by Unmapped IDs: {results['unmapped_affected_rows']:,} ({results['unmapped_pct']:.2f}%)")


if __name__ == "__main__":
    main()
