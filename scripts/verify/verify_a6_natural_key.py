import sys
from pathlib import Path
sys.path.insert(0, "/home/saniya/flight-delay-analytics")

from src.utils.spark_session import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("natural_key_audit")
df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_clean")

print("=" * 80)
print("AUDITING NATURAL KEY DUPLICATES")
print("=" * 80)

# 1. 8-column key including DESTINATION_AIRPORT_RAW
group_cols_8 = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
    "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW", "SCHEDULED_DEPARTURE"
]
print(f"Checking duplicate groups on 8-column key: {group_cols_8}")
dup_groups_8 = df.groupBy(group_cols_8).count().filter(F.col("count") > 1)
count_groups_8 = dup_groups_8.count()
print(f"Duplicate groups found on 8-column key (with DESTINATION): {count_groups_8}")

# 2. 7-column key without DESTINATION (as evaluated in Phase 3)
group_cols_7 = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
    "ORIGIN_AIRPORT_RAW", "SCHEDULED_DEPARTURE"
]
print(f"\nChecking duplicate groups on 7-column key (without DESTINATION): {group_cols_7}")
dup_groups_7 = df.groupBy(group_cols_7).count().filter(F.col("count") > 1)
count_groups_7 = dup_groups_7.count()
print(f"Duplicate groups found on 7-column key: {count_groups_7}")

if count_groups_7 > 0:
    print("\nRows matching the 7-column key duplicate:")
    dup_rows = df.join(dup_groups_7.select(group_cols_7), on=group_cols_7, how="inner")
    dup_rows.select(
        "FLIGHT_DATE", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER",
        "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW", "SCHEDULED_DEPARTURE",
        "SCHEDULED_ARRIVAL", "DISTANCE", "CANCELLED", "CANCELLATION_REASON"
    ).show(truncate=False)

spark.stop()
