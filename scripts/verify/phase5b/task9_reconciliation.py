#!/usr/bin/env python3
"""
scripts/verify/phase5b/task9_reconciliation.py - Strict row-by-row reconciliation
between output/analytics/phase5_insights.md and processed/analytics/kpi_by_airline.
"""
import sys
import re
from pathlib import Path

PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session
import pyspark.sql.functions as F

spark = get_spark_session("task9_reconciliation")
df_airline = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/analytics/kpi_by_airline")

print("=" * 80)
print("TASK 9: AIRLINE REPORT ROW-BY-ROW RECONCILIATION")
print("=" * 80)

# 1. Read Markdown Report
md_path = PROJECT_ROOT / "output" / "analytics" / "phase5_insights.md"
with open(md_path, "r", encoding="utf-8") as f:
    md_content = f.read()

# Pattern: | Rank | `CODE` | Name | Total Flights | Flown Flights | On-Time Rate | Arr Delay Rate (>=15m) | Cancel Rate |
air_pattern = re.compile(
    r"\|\s*(\d+)\s*\|\s*`([A-Z0-9]+)`\s*\|\s*([^|]+)\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|\s*\*\*([0-9.]+)%\*\*\s*\|\s*([0-9.]+)%\s*\|\s*([0-9.]+)%\s*\|"
)
md_rows = air_pattern.findall(md_content)

print(f"Found {len(md_rows)} airline table rows in {md_path} (Top 5 and Bottom 5 non-overlapping):")
print(f"{'Rank':4s} {'Code':4s} {'Name':28s} {'Total':11s} {'Flown':11s} {'On-Time':9s} {'ArrDelay':9s} {'Cancel':9s}")
print("-" * 90)

tbl_rows = {r["AIRLINE"]: r for r in df_airline.collect()}

all_matched = True
md_flown_sum = 0
md_total_sum = 0

for rank, code, name, tot_str, flown_str, ot_str, arr_str, canc_str in md_rows:
    tot = int(tot_str.replace(",", ""))
    flown = int(flown_str.replace(",", ""))
    ot_pct = float(ot_str)
    arr_pct = float(arr_str)
    canc_pct = float(canc_str)
    
    md_total_sum += tot
    md_flown_sum += flown
    
    t = tbl_rows.get(code)
    if not t:
        print(f"[FAIL] Code {code} missing from parquet table!")
        all_matched = False
        continue
    
    exp_tot = t["total_flights"]
    exp_flown = t["flown_flights"]
    exp_ot = round(t["on_time_rate"] * 100, 2)
    exp_arr = round(t["arr_delay_rate_15"] * 100, 2)
    exp_canc = round(t["cancel_rate"] * 100, 2)
    
    tot_ok = (tot == exp_tot)
    flown_ok = (flown == exp_flown)
    ot_ok = (abs(ot_pct - exp_ot) <= 0.02)
    arr_ok = (abs(arr_pct - exp_arr) <= 0.02)
    canc_ok = (abs(canc_pct - exp_canc) <= 0.02)
    
    status = "OK" if (tot_ok and flown_ok and ot_ok and arr_ok and canc_ok) else "MISMATCH"
    if status != "OK":
        all_matched = False
    
    print(f"{rank:4s} {code:4s} {name.strip():28s} {tot:11,d} {flown:11,d} {ot_pct:8.2f}% {arr_pct:8.2f}% {canc_pct:8.2f}% [{status}]")

print("-" * 90)
print(f"Markdown Sample Total Sum (Top 5 + Bottom 5): {md_total_sum:,d}")
print(f"Markdown Sample Flown Sum (Top 5 + Bottom 5): {md_flown_sum:,d}")
assert all_matched, "Some markdown rows mismatched table data!"

# Verify that all cancel rates in markdown are strictly cancelled/total:
for rank, code, name, tot_str, flown_str, ot_str, arr_str, canc_str in md_rows:
    tot = int(tot_str.replace(",", ""))
    canc_pct = float(canc_str)
    t = tbl_rows[code]
    expected_rate_pct = round(t["cancelled_flights"] / t["total_flights"] * 100, 2)
    assert abs(canc_pct - expected_rate_pct) <= 0.02, f"Cancel rate mismatch for {code}: MD={canc_pct}%, Exp={expected_rate_pct}%"

print("\n[SUCCESS] Row-by-row cross-check verified! All values match kpi_by_airline and cancel rates are strictly cancelled/total.")
spark.stop()
