import sys
import time
from pathlib import Path

project_root = Path.home() / "flight-delay-analytics"
sys.path.insert(0, str(project_root))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F

spark = get_spark_session(
    app_name="Phase4-PartA-Verification",
    enable_hdfs=True,
    extra_configs={
        "spark.master": "local[6]",
        "spark.driver.memory": "5g",
        "spark.sql.shuffle.partitions": "24",
    }
)

print("============================================================")
print("PART A1: RECONCILE UNMAPPED ROWS")
print("============================================================")

df_clean = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_clean")
df_map = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/airport_id_map")

# 1. Unmapped counts
orig_unmapped = df_clean.filter(F.col("ORIGIN_UNMAPPED") == 1).count()
dest_unmapped = df_clean.filter(F.col("DEST_UNMAPPED") == 1).count()
both_unmapped = df_clean.filter((F.col("ORIGIN_UNMAPPED") == 1) & (F.col("DEST_UNMAPPED") == 1)).count()
either_unmapped = df_clean.filter((F.col("ORIGIN_UNMAPPED") == 1) | (F.col("DEST_UNMAPPED") == 1)).count()

print("ORIGIN_UNMAPPED=1 rows: " + str(orig_unmapped))
print("DEST_UNMAPPED=1 rows:   " + str(dest_unmapped))
print("Both unmapped rows:     " + str(both_unmapped))
print("Either unmapped rows:   " + str(either_unmapped))
identity_calc = orig_unmapped + dest_unmapped - both_unmapped
print("Identity Check: " + str(orig_unmapped) + " + " + str(dest_unmapped) + " - " + str(both_unmapped) + " = " + str(identity_calc) + " (Matches either: " + str(identity_calc == either_unmapped) + ")")

# 2. Per-ID table for the 11 unmapped IDs
oct_flights = df_clean.filter(F.col("MONTH") == 10)
mapped_ids = set(r["airport_id"] for r in df_map.select("airport_id").collect())

all_numeric_orig = set(r["ORIGIN_AIRPORT_RAW"] for r in oct_flights.filter(F.col("ORIGIN_AIRPORT_RAW").rlike(r"^[0-9]+$")).select("ORIGIN_AIRPORT_RAW").distinct().collect())
all_numeric_dest = set(r["DESTINATION_AIRPORT_RAW"] for r in oct_flights.filter(F.col("DESTINATION_AIRPORT_RAW").rlike(r"^[0-9]+$")).select("DESTINATION_AIRPORT_RAW").distinct().collect())
all_numeric = sorted(all_numeric_orig | all_numeric_dest)
unmapped_ids = sorted([i for i in all_numeric if i not in mapped_ids])

print("")
print("Total numeric IDs found in October: " + str(len(all_numeric)))
print("Mapped IDs: " + str(len(mapped_ids)))
print("Unmapped IDs (" + str(len(unmapped_ids)) + " total): " + str(unmapped_ids))

print("")
print("Per-ID Table for 11 Unmapped IDs:")
print(f"{'ID':<8} | {'rows_as_origin':<14} | {'rows_as_dest':<14} | {'rows_total (sum)':<16} | {'rows_either (distinct)':<22}")
print("-" * 85)

tot_orig = 0
tot_dest = 0
tot_sum = 0

for uid in unmapped_ids:
    c_orig = oct_flights.filter(F.col("ORIGIN_AIRPORT_RAW") == uid).count()
    c_dest = oct_flights.filter(F.col("DESTINATION_AIRPORT_RAW") == uid).count()
    c_either = oct_flights.filter((F.col("ORIGIN_AIRPORT_RAW") == uid) | (F.col("DESTINATION_AIRPORT_RAW") == uid)).count()
    tot_orig += c_orig
    tot_dest += c_dest
    tot_sum += (c_orig + c_dest)
    print(f"{uid:<8} | {c_orig:<14} | {c_dest:<14} | {c_orig + c_dest:<16} | {c_either:<22}")

print("-" * 85)
print(f"{'TOTAL':<8} | {tot_orig:<14} | {tot_dest:<14} | {tot_sum:<16} | {either_unmapped:<22}")

print("")
print("============================================================")
print("PART A2: NORMALIZATION, DUPLICATES, AND SPECIAL ROWS")
print("============================================================")

df_raw = spark.read.csv("hdfs://localhost:9000/user/saniya/flight_delay/raw/flights/flights.csv", header=True, inferSchema=False)
time_cols = ["SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "WHEELS_OFF", "WHEELS_ON", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME"]

# Count 2400 in raw
raw_2400_exprs = [F.sum(F.when(F.col(c) == "2400", 1).otherwise(0)).alias(c) for c in time_cols]
raw_2400_counts = df_raw.agg(*raw_2400_exprs).collect()[0].asDict()

phase2_reported = {
    "SCHEDULED_DEPARTURE": 0,
    "DEPARTURE_TIME": 513,
    "WHEELS_OFF": 727,
    "WHEELS_ON": 2005,
    "SCHEDULED_ARRIVAL": 2,
    "ARRIVAL_TIME": 2456
}

print("Count of '2400' values normalized:")
print(f"{'Column':<22} | {'Raw 2400 Count':<16} | {'Phase 2 Reported':<18} | {'Difference':<12}")
print("-" * 75)
for tc in time_cols:
    act = int(raw_2400_counts[tc])
    exp = phase2_reported[tc]
    diff = act - exp
    print(f"{tc:<22} | {act:<16} | {exp:<18} | {diff:<12}")

# HAS_2400_TIME in clean
has_2400_cnt = df_clean.filter(F.col("HAS_2400_TIME") == 1).count()
print("")
print("HAS_2400_TIME=1 row count in flights_clean: " + str(has_2400_cnt))

# Exact duplicates dropped
raw_total = df_raw.count()
clean_total = df_clean.count()
exact_dupes_dropped = raw_total - clean_total
print("")
print(f"Raw total rows: {raw_total:,}")
print(f"Clean total rows: {clean_total:,}")
print(f"Exact duplicates dropped: {exact_dupes_dropped}")

# DUP_NATURAL_KEY=1 rows
print("")
print("--- Rows with DUP_NATURAL_KEY=1 (All Columns) ---")
dup_rows = df_clean.filter(F.col("DUP_NATURAL_KEY") == 1)
print(f"Total DUP_NATURAL_KEY=1 rows: {dup_rows.count()}")
for i, r in enumerate(dup_rows.collect(), 1):
    print("")
    print(f"[Duplicate Natural Key Row #{i}]")
    d = r.asDict()
    for k in sorted(d.keys()):
        print(f"  {k}: {d[k]}")

# Null SCHEDULED_TIME rows
print("")
print("--- The 6 rows with null SCHEDULED_TIME (All Columns) ---")
null_sched = df_clean.filter(F.col("SCHEDULED_TIME").isNull())
print(f"Total null SCHEDULED_TIME rows: {null_sched.count()}")
for i, r in enumerate(null_sched.collect(), 1):
    print("")
    print(f"[Null SCHEDULED_TIME Row #{i}]")
    d = r.asDict()
    for k in sorted(d.keys()):
        print(f"  {k}: {d[k]}")

spark.stop()
