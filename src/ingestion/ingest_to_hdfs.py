#!/usr/bin/env python3
"""
ingest_to_hdfs.py - Ingest raw flight delay datasets into HDFS.
Idempotently uploads raw CSVs to dedicated HDFS directories and verifies byte integrity.
"""

import os
import sys
import argparse
import logging
import subprocess
import socket
from pathlib import Path
from typing import Dict, Any, Tuple, List

try:
    import yaml
except ImportError:
    yaml = None


def get_project_root() -> Path:
    """Returns the absolute root directory of the project."""
    return Path(__file__).resolve().parent.parent.parent


def setup_logging(project_root: Path) -> logging.Logger:
    """Configures logging to both console and logs/ingestion.log."""
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "ingestion.log"

    logger = logging.getLogger("ingest_to_hdfs")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # File handler
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def load_configuration(project_root: Path) -> Dict[str, Any]:
    """Loads settings from config/config.yaml without hardcoding paths."""
    config_path = project_root / "config" / "config.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    if yaml is None:
        raise ImportError("PyYAML is required to load configuration settings.")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def check_hdfs_availability(logger: logging.Logger, host: str = "localhost", port: int = 9000) -> None:
    """Verifies that the HDFS NameNode IPC port is reachable and safemode is OFF."""
    logger.info(f"Checking HDFS NameNode availability at {host}:{port}...")
    try:
        with socket.create_connection((host, port), timeout=3.0):
            pass
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        logger.error(f"[FATAL] HDFS NameNode is unreachable at {host}:{port}: {e}")
        raise RuntimeError(
            f"HDFS NameNode is unreachable at {host}:{port}. "
            "Please run '~/flight-delay-analytics/scripts/start_hdfs.sh' first."
        ) from e

    # Check safemode status
    cmd = ["hdfs", "dfsadmin", "-safemode", "get"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        if "Safe mode is ON" in proc.stdout:
            logger.error("[FATAL] HDFS NameNode is in SAFE MODE. Cannot ingest data.")
            raise RuntimeError("HDFS NameNode is currently in SAFE MODE.")
        logger.info(f"HDFS NameNode reachable and ready: {proc.stdout.strip()}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to check safemode: {e.stderr}")
        raise RuntimeError(f"Failed to execute hdfs dfsadmin: {e.stderr}") from e


def get_hdfs_file_size(hdfs_path: str, logger: logging.Logger) -> int:
    """Returns the size of an HDFS file in bytes using 'hdfs dfs -stat %b'."""
    cmd = ["hdfs", "dfs", "-stat", "%b", hdfs_path]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        logger.error(f"Failed to get size for HDFS file {hdfs_path}: {proc.stderr.strip()}")
        raise RuntimeError(f"HDFS file {hdfs_path} not found or inaccessible: {proc.stderr.strip()}")
    
    # Filter out any warnings from Hadoop and get the last line which contains the size
    lines = [line.strip() for line in proc.stdout.strip().splitlines() if line.strip() and not line.startswith("202")]
    if not lines:
        raise ValueError(f"Could not parse size output from hdfs dfs -stat: {proc.stdout}")
    
    return int(lines[-1])


def ingest_and_validate(verify_only: bool = False) -> None:
    """Main ingestion and validation routine."""
    project_root = get_project_root()
    logger = setup_logging(project_root)

    logger.info("=" * 60)
    mode = "VALIDATION ONLY" if verify_only else "INGESTION & VALIDATION"
    logger.info(f"Starting HDFS Raw Data Ingestion Pipeline [{mode}]")
    logger.info("=" * 60)

    config = load_configuration(project_root)
    hdfs_cfg = config.get("hdfs", {})
    hdfs_paths = hdfs_cfg.get("paths", {})
    raw_data_cfg = config.get("raw_data", {})

    local_dir = project_root / raw_data_cfg.get("local_dir", "data/raw")
    file_names = raw_data_cfg.get("files", {})

    # Extract host and port from namenode_uri
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    host = "localhost"
    port = 9000
    if "://" in namenode_uri:
        netloc = namenode_uri.split("://", 1)[1].split("/")[0]
        if ":" in netloc:
            parts = netloc.split(":")
            host = parts[0]
            port = int(parts[1])
        else:
            host = netloc

    check_hdfs_availability(logger, host, port)

    # Ingestion targets specification
    targets: List[Tuple[str, Path, str, str]] = [
        (
            "airlines",
            local_dir / file_names.get("airlines", "airlines.csv"),
            hdfs_paths.get("airlines_subfolder", "/user/saniya/flight_delay/raw/airlines"),
            hdfs_paths.get("airlines", "/user/saniya/flight_delay/raw/airlines/airlines.csv"),
        ),
        (
            "airports",
            local_dir / file_names.get("airports", "airports.csv"),
            hdfs_paths.get("airports_subfolder", "/user/saniya/flight_delay/raw/airports"),
            hdfs_paths.get("airports", "/user/saniya/flight_delay/raw/airports/airports.csv"),
        ),
        (
            "flights",
            local_dir / file_names.get("flights", "flights.csv"),
            hdfs_paths.get("flights_subfolder", "/user/saniya/flight_delay/raw/flights"),
            hdfs_paths.get("flights", "/user/saniya/flight_delay/raw/flights/flights.csv"),
        ),
    ]

    for name, local_file, hdfs_dir, hdfs_dest in targets:
        logger.info(f"--- Processing dataset: {name} ---")

        if not local_file.exists():
            logger.error(f"[FATAL] Required local file missing: {local_file}")
            raise FileNotFoundError(f"Local file does not exist: {local_file}")

        local_size = local_file.stat().st_size
        logger.info(f"Local file: {local_file} ({local_size:,} bytes)")

        if not verify_only:
            # Create target HDFS subfolder idempotently
            logger.info(f"Ensuring HDFS directory exists: {hdfs_dir}")
            mkdir_cmd = ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir]
            proc_mkdir = subprocess.run(mkdir_cmd, capture_output=True, text=True)
            if proc_mkdir.returncode != 0:
                logger.error(f"Failed to create HDFS directory {hdfs_dir}: {proc_mkdir.stderr}")
                raise RuntimeError(f"HDFS mkdir failed: {proc_mkdir.stderr}")

            # Upload with overwrite flag (-f)
            logger.info(f"Uploading {local_file.name} to {hdfs_dest} via 'hdfs dfs -put -f'...")
            put_cmd = ["hdfs", "dfs", "-put", "-f", str(local_file), hdfs_dest]
            proc_put = subprocess.run(put_cmd, capture_output=True, text=True)
            if proc_put.returncode != 0:
                logger.error(f"HDFS upload failed for {name}: {proc_put.stderr}")
                raise RuntimeError(f"HDFS put failed for {name}: {proc_put.stderr}")
            logger.info(f"Upload completed successfully for {name}.")

        # Verification step: compare byte sizes
        logger.info(f"Verifying byte size in HDFS for {hdfs_dest}...")
        hdfs_size = get_hdfs_file_size(hdfs_dest, logger)
        logger.info(f"HDFS file size: {hdfs_size:,} bytes")

        if local_size != hdfs_size:
            err_msg = (
                f"[FATAL] Byte size mismatch for {name}! "
                f"Local: {local_size:,} bytes vs HDFS: {hdfs_size:,} bytes."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)

        logger.info(f"[SUCCESS] {name}: Local ({local_size:,} bytes) == HDFS ({hdfs_size:,} bytes) MATCH!")

    logger.info("=" * 60)
    logger.info("[COMPLETE] All raw datasets successfully ingested and verified in HDFS.")
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw flight delay CSVs into HDFS with byte-level verification."
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Skip upload and only verify existence and size of files in HDFS against local files.",
    )
    args = parser.parse_args()

    try:
        ingest_and_validate(verify_only=args.verify_only)
    except Exception as e:
        print(f"\n[FATAL ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
