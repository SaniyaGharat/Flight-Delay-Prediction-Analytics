import os
import sys
from pathlib import Path

PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("both_unmapped_check")
df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_clean")

print("=== 62 Both-Unmapped Rows Grouped by (ORIGIN_AIRPORT_RAW, DESTINATION_AIRPORT_RAW) ===")
both_unmapped = df.filter((F.col("ORIGIN_UNMAPPED") == 1) & (F.col("DEST_UNMAPPED") == 1))
print(f"Total rows with both unmapped: {both_unmapped.count()}")

both_unmapped.groupBy("ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW") \
    .count() \
    .orderBy(F.desc("count"), "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW") \
    .show(50, truncate=False)

spark.stop()
