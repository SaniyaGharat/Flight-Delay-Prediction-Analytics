"""
Reusable SparkSession builder for the Flight Delay Prediction and Analytics pipeline.
Configures HDFS default filesystem, memory settings, and shuffle partitions for local execution.
Linux / WSL2 production runtime (no Windows reflection patches or winutils).
"""

import os
import sys
import socket
from pathlib import Path
from typing import Optional, Dict, Any

try:
    import yaml
except ImportError:
    yaml = None

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def _get_project_root() -> Path:
    """Returns the root directory of the flight-delay-analytics project."""
    return Path(__file__).resolve().parent.parent.parent


def configure_environment() -> None:
    """
    Ensures JAVA_HOME and HADOOP_HOME are set for the Linux/WSL2 runtime.
    Defaults to system OpenJDK 17 and ~/hadoop if not explicitly provided.
    """
    project_root = _get_project_root()

    # Load environment variables from .env if present
    env_file = project_root / ".env"
    if env_file.exists() and load_dotenv:
        load_dotenv(env_file, override=False)

    # Set JAVA_HOME if unset
    if "JAVA_HOME" not in os.environ or not Path(os.environ["JAVA_HOME"]).exists():
        default_java = Path("/usr/lib/jvm/java-17-openjdk-amd64")
        if default_java.exists():
            os.environ["JAVA_HOME"] = str(default_java)

    # Set HADOOP_HOME if unset
    if "HADOOP_HOME" not in os.environ or not Path(os.environ["HADOOP_HOME"]).exists():
        user_home = Path.home()
        default_hadoop = user_home / "hadoop"
        if default_hadoop.exists():
            os.environ["HADOOP_HOME"] = str(default_hadoop)

    # Set HADOOP_CONF_DIR to align Spark with native Hadoop site configurations
    if "HADOOP_HOME" in os.environ:
        hadoop_conf = str(Path(os.environ["HADOOP_HOME"]) / "etc" / "hadoop")
        os.environ.setdefault("HADOOP_CONF_DIR", hadoop_conf)

    # Ensure Hadoop bin & sbin are on PATH
    if "HADOOP_HOME" in os.environ:
        hadoop_bin = str(Path(os.environ["HADOOP_HOME"]) / "bin")
        hadoop_sbin = str(Path(os.environ["HADOOP_HOME"]) / "sbin")
        current_path = os.environ.get("PATH", "")
        if hadoop_bin not in current_path:
            os.environ["PATH"] = f"{hadoop_bin}:{hadoop_sbin}:{current_path}"

    # Align PySpark worker and driver with the active Python virtual environment
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# Run environment configuration on module import
configure_environment()

from pyspark.sql import SparkSession


def load_config() -> Dict[str, Any]:
    """Loads configuration settings from config/config.yaml."""
    project_root = _get_project_root()
    config_path = project_root / "config" / "config.yaml"

    defaults: Dict[str, Any] = {
        "spark": {
            "master": "local[*]",
            "driver_memory": "4g",
            "executor_memory": "4g",
            "shuffle_partitions": 8,
            "default_fs": "hdfs://localhost:9000",
        },
        "hdfs": {
            "namenode_uri": "hdfs://localhost:9000",
            "smoke_test_dir": "/user/{USER}/flight_delay/tmp_smoke_test",
        },
    }

    if config_path.exists() and yaml is not None:
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f)
                if loaded and isinstance(loaded, dict):
                    if "spark" in loaded:
                        defaults["spark"].update(loaded["spark"])
                    if "hdfs" in loaded:
                        defaults["hdfs"].update(loaded["hdfs"])
                    return defaults
        except Exception as e:
            print(f"[WARN] Failed to read {config_path}: {e}. Using defaults.")

    return defaults


def is_hdfs_available(host: str = "localhost", port: int = 9000, timeout_sec: float = 2.0) -> bool:
    """Checks whether the HDFS NameNode IPC port is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def get_spark_session(
    app_name: str = "FlightDelayPredictionAndAnalytics",
    enable_hdfs: bool = True,
    extra_configs: Optional[Dict[str, str]] = None,
) -> SparkSession:
    """
    Creates and returns a tuned SparkSession for local execution with HDFS integration.
    Fails loudly if HDFS is requested but unreachable.

    Parameters:
        app_name (str): Spark application name displayed in Spark UI.
        enable_hdfs (bool): Whether to configure HDFS (hdfs://localhost:9000) as defaultFS.
        extra_configs (dict): Additional key-value configuration pairs.

    Returns:
        SparkSession: The initialized SparkSession instance.

    Raises:
        RuntimeError: If enable_hdfs is True and the NameNode IPC endpoint is unreachable.
    """
    configure_environment()

    config = load_config()
    spark_cfg = config.get("spark", {})
    hdfs_uri = spark_cfg.get("default_fs", "hdfs://localhost:9000")

    builder = SparkSession.builder.appName(app_name)

    # Master configuration for single machine execution
    builder = builder.master(spark_cfg.get("master", "local[*]"))

    # Memory settings for local analytics workstation
    builder = builder.config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
    builder = builder.config("spark.executor.memory", spark_cfg.get("executor_memory", "4g"))

    # Single-machine shuffle optimization
    shuffle_parts = str(spark_cfg.get("shuffle_partitions", 8))
    builder = builder.config("spark.sql.shuffle.partitions", shuffle_parts)
    builder = builder.config("spark.default.parallelism", shuffle_parts)

    # High performance PyArrow columnar conversion
    builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Timezone consistency
    builder = builder.config("spark.sql.session.timeZone", "UTC")

    # Configure HDFS default filesystem
    if enable_hdfs:
        host = "localhost"
        port = 9000
        if "://" in hdfs_uri:
            netloc = hdfs_uri.split("://", 1)[1].split("/")[0]
            if ":" in netloc:
                parts = netloc.split(":")
                host = parts[0]
                try:
                    port = int(parts[1])
                except ValueError:
                    port = 9000
            else:
                host = netloc

        if not is_hdfs_available(host, port):
            raise RuntimeError(
                f"[FATAL] HDFS NameNode is unreachable at {host}:{port}! "
                f"Cannot configure fs.defaultFS to {hdfs_uri}. "
                f"Ensure HDFS daemons are started via 'start-dfs.sh' before launching Spark."
            )

        print(f"[INFO] HDFS NameNode reachable at {host}:{port}. Setting fs.defaultFS = {hdfs_uri}")
        builder = builder.config("spark.hadoop.fs.defaultFS", hdfs_uri)
        # Enforce replication factor 1 for local single-node HDFS cluster
        builder = builder.config("spark.hadoop.dfs.replication", "1")

    # Apply any custom extra configurations
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, str(value))

    spark = builder.getOrCreate()

    # Set log level to WARN to reduce verbosity in interactive workflows
    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == "__main__":
    print("[INFO] Initializing SparkSession via get_spark_session()...")
    session = get_spark_session("FlightDelayAnalytics-SelfTest")
    print(f"[SUCCESS] SparkSession created: version={session.version}, master={session.sparkContext.master}")
    print(f"[SUCCESS] defaultFS: {session.conf.get('spark.hadoop.fs.defaultFS')}")
    session.stop()
