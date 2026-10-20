"""
airline_table_check.py - Reads processed/analytics/kpi_by_airline and prints all 14 rows + sums.
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("airline_check")
df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/analytics/kpi_by_airline")

print("=" * 100)
print("KPI_BY_AIRLINE STORED TABLE (14 ROWS)")
print("=" * 100)
cols = ["AIRLINE", "AIRLINE_NAME", "total_flights", "flown_flights", "cancelled_flights", "diverted_flights", "cancel_rate", "on_time_rate"]
df_sorted = df.select(cols).orderBy(F.desc("total_flights"))

rows = df_sorted.collect()
print(f"{'AIRLINE':<8} {'TOTAL':<12} {'FLOWN':<12} {'CANCELLED':<10} {'DIVERTED':<10} {'CANCEL_RATE':<12} {'ON_TIME_RATE':<12} {'NAME'}")
print("-" * 100)
for r in rows:
    print(f"{r['AIRLINE']:<8} {r['total_flights']:<12,d} {r['flown_flights']:<12,d} {r['cancelled_flights']:<10,d} {r['diverted_flights']:<10,d} {r['cancel_rate']:<12.6f} {r['on_time_rate']:<12.6f} {r['AIRLINE_NAME']}")

print("-" * 100)
sums = df.agg(
    F.sum("total_flights").alias("tot"),
    F.sum("flown_flights").alias("flown"),
    F.sum("cancelled_flights").alias("canc"),
    F.sum("diverted_flights").alias("div")
).collect()[0]
print(f"{'SUM':<8} {sums['tot']:<12,d} {sums['flown']:<12,d} {sums['canc']:<10,d} {sums['div']:<10,d}")
print("=" * 100)

assert sums['tot'] == 5819079, f"Total flights mismatch: {sums['tot']}"
assert sums['flown'] == 5714008, f"Flown flights mismatch: {sums['flown']}"
assert sums['canc'] == 89884, f"Cancelled flights mismatch: {sums['canc']}"
assert sums['div'] == 15187, f"Diverted flights mismatch: {sums['div']}"
print("ASSERTIONS PASSED: Table sums match 5,819,079 / 5,714,008 / 89,884 / 15,187 exactly!")

spark.stop()
