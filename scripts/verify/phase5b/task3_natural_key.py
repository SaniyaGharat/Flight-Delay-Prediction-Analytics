#!/usr/bin/env python3
"""
scripts/verify/phase5b/task3_natural_key.py - Print all columns of the STT duplicate rows.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("task3_natural_key")
df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_clean")

print("=" * 80)
print("TASK 3: NATURAL KEY DUPLICATE AUDIT (ALL COLUMNS)")
print("=" * 80)

# Check 8-column key (with DESTINATION_AIRPORT_RAW)
group_cols_8 = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
    "ORIGIN_AIRPORT_RAW", "DESTINATION_AIRPORT_RAW", "SCHEDULED_DEPARTURE"
]
dup_groups_8 = df.groupBy(group_cols_8).count().filter(F.col("count") > 1)
print(f"Duplicate groups on 8-column key (with DESTINATION): {dup_groups_8.count()}")

# Check 7-column key (without DESTINATION_AIRPORT_RAW)
group_cols_7 = [
    "YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER",
    "ORIGIN_AIRPORT_RAW", "SCHEDULED_DEPARTURE"
]
dup_groups_7 = df.groupBy(group_cols_7).count().filter(F.col("count") > 1)
print(f"Duplicate groups on 7-column key (without DESTINATION): {dup_groups_7.count()}")

dup_rows = df.join(dup_groups_7.select(group_cols_7), on=group_cols_7, how="inner").collect()
print(f"\nFound {len(dup_rows)} duplicate rows. Printing ALL {len(df.columns)} columns for each row:\n")

for i, row in enumerate(dup_rows, 1):
    print(f"--- ROW {i} (DESTINATION = {row['DESTINATION_AIRPORT_RAW']}) ---")
    d = row.asDict()
    for col_name in df.columns:
        print(f"  {col_name:25s}: {d[col_name]}")
    print()

print("=" * 80)
print("ANALYSIS & CORRECTION:")
print("Both flights are on 2015-08-29, Airline AA, Flight 803, Origin STT, SchedDep 1435.")
print("Row 1 Destination is SJU (Distance 68, SchedArr 1500).")
print("Row 2 Destination is CLT (Distance 1520, SchedArr 1827).")
print("Both rows have CANCELLED = 1 and CANCELLATION_REASON = 'A' (Carrier).")
print("Because both flights share the exact same scheduled departure time (14:35) from the same airport (STT)")
print("to different destinations and both were cancelled, this is NOT a multi-leg sequential flight.")
print("It represents a dual-scheduled / re-routed flight record that was cancelled by the carrier.")
print("=" * 80)

spark.stop()
