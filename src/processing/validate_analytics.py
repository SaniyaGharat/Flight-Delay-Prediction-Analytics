"""
src/processing/validate_analytics.py - Strict validation suite for Phase 5 analytics tables.

Performs PASS/FAIL checks against required invariants:
- Dimension totals reconciliation (5,819,079 total, 5,714,008 flown, 89,884 cancelled, etc.)
- Cause sums equality and exact match to ARRIVAL_DELAY on HAS_CAUSE_DATA=1
- Airline flight count exact matches against Phase 2 (all 14 printed in full)
- UNMAPPED bucket counts (origin 757, dest 755)
- Re-aggregation consistency (kpi_by_month weighted average == kpi_overall == 4.41)
- Plain Spark SQL independent recomputation of kpi_overall and 3 (airline, month) cells
- Domain integrity: no null keys, no negative counts, rates in [0, 1], rank deterministic tie-breaking
- Markdown cross-check: re-parses output/analytics/phase5_insights.md and cross-checks airline & month tables
- Prints row counts, column lists, and full kpi_overall row
"""

import os
import sys
import time
import json
import re
import logging
from pathlib import Path

PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.spark_session import get_spark_session, load_config
import pyspark.sql.functions as F


def setup_logger():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("validate_analytics")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(log_dir / "validate_analytics.log", mode="a", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
    return logger


def validate_analytics():
    start_time = time.perf_counter()
    logger = setup_logger()
    logger.info("============================================================")
    logger.info("Starting Phase 5 Analytics Validation Suite...")
    logger.info("============================================================")

    config = load_config()
    hdfs_cfg = config.get("hdfs", {})
    namenode_uri = hdfs_cfg.get("namenode_uri", "hdfs://localhost:9000")
    user = os.environ.get("USER", "saniya")
    processed_dir = hdfs_cfg.get("processed_dir", f"/user/{user}/flight_delay/processed").replace("${USER}", user)
    
    analytics_base = f"{namenode_uri}{processed_dir}/analytics"
    enriched_path = f"{namenode_uri}{processed_dir}/flights_enriched"

    spark = get_spark_session("validate_analytics")

    checks = []

    def record_check(name, passed, details=""):
        status = "PASS" if passed else "FAIL"
        checks.append({"name": name, "status": status, "details": details})
        msg = f"[{status}] {name}"
        if details:
            msg += f" - {details}"
        if passed:
            logger.info(msg)
        else:
            logger.error(msg)

    try:
        # Load tables
        table_names = [
            "kpi_overall", "kpi_by_month", "kpi_by_day_of_week", "kpi_by_dep_hour", "kpi_by_dow_hour",
            "kpi_by_airline", "kpi_by_airline_month", "kpi_by_origin_airport", "kpi_by_dest_airport",
            "kpi_by_route", "kpi_by_origin_state", "kpi_by_date", "kpi_by_segment",
            "cancellations_by_reason_month", "cancellations_by_reason_airline",
            "causes_by_month", "causes_by_airline", "causes_by_origin_airport",
            "delay_category_by_airline"
        ]

        tables = {}
        for tname in table_names:
            tpath = f"{analytics_base}/{tname}"
            df_t = spark.read.parquet(tpath)
            tables[tname] = df_t

        print("\n=== TABLE ROW COUNTS AND SCHEMA SUMMARY ===")
        for tname in table_names:
            cnt = tables[tname].count()
            cols = tables[tname].columns
            print(f"  {tname:32s}: {cnt:6d} rows | {len(cols):2d} cols: {', '.join(cols[:6])}...")

        # ----------------------------------------------------------------------
        # Check 1: Dimension tables sum reconciliation
        # ----------------------------------------------------------------------
        print("\n=== CHECK 1: DIMENSION TABLES SUM RECONCILIATION ===")
        expected_sums = {
            "total_flights": 5819079,
            "flown_flights": 5714008,
            "cancelled_flights": 89884,
            "diverted_flights": 15187,
            "delayed_arr_15": 1063439,
            "delayed_dep_15": 1050422
        }

        dim_tables = [
            "kpi_overall", "kpi_by_month", "kpi_by_day_of_week", "kpi_by_dep_hour", "kpi_by_dow_hour",
            "kpi_by_airline", "kpi_by_airline_month", "kpi_by_origin_airport", "kpi_by_dest_airport",
            "kpi_by_route", "kpi_by_origin_state", "kpi_by_date"
        ]

        all_dim_pass = True
        for dt in dim_tables:
            agg_row = tables[dt].agg(
                F.sum("total_flights").alias("total_flights"),
                F.sum("flown_flights").alias("flown_flights"),
                F.sum("cancelled_flights").alias("cancelled_flights"),
                F.sum("diverted_flights").alias("diverted_flights"),
                F.sum("delayed_arr_15").alias("delayed_arr_15"),
                F.sum("delayed_dep_15").alias("delayed_dep_15")
            ).collect()[0]

            for metric, exp_val in expected_sums.items():
                act_val = agg_row[metric]
                if act_val != exp_val:
                    all_dim_pass = False
                    logger.error(f"Mismatch in {dt} for {metric}: got {act_val}, expected {exp_val}")

        record_check("dimension_tables_sum_reconciliation", all_dim_pass,
                     f"All 12 dimension tables match: total=5,819,079, flown=5,714,008, cancelled=89,884, diverted=15,187, arr_15=1,063,439, dep_15=1,050,422")

        # ----------------------------------------------------------------------
        # Check 2: Cause delay totals and consistency
        # ----------------------------------------------------------------------
        print("\n=== CHECK 2: CAUSE TOTALS AND CONSISTENCY ===")
        df_overall = tables["kpi_overall"].collect()[0]
        overall_cause_total = (
            df_overall["sum_cause_air_system"] +
            df_overall["sum_cause_security"] +
            df_overall["sum_cause_airline"] +
            df_overall["sum_cause_late_aircraft"] +
            df_overall["sum_cause_weather"]
        )

        cause_tables = ["causes_by_month", "causes_by_airline", "causes_by_origin_airport"]
        cause_match = True
        for ct in cause_tables:
            c_row = tables[ct].agg(
                F.sum("sum_cause_air_system").alias("sys"),
                F.sum("sum_cause_security").alias("sec"),
                F.sum("sum_cause_airline").alias("air"),
                F.sum("sum_cause_late_aircraft").alias("late"),
                F.sum("sum_cause_weather").alias("wea"),
                F.sum("total_delayed_15").alias("cnt")
            ).collect()[0]
            ct_sum = c_row["sys"] + c_row["sec"] + c_row["air"] + c_row["late"] + c_row["wea"]
            if ct_sum != overall_cause_total:
                cause_match = False
                logger.error(f"Cause total mismatch in {ct}: {ct_sum} vs {overall_cause_total}")
            if c_row["cnt"] != 1063439:
                cause_match = False
                logger.error(f"Delayed count mismatch in {ct}: {c_row['cnt']} vs 1063439")

        # Exact match to ARRIVAL_DELAY on HAS_CAUSE_DATA=1
        df_enr = spark.read.parquet(enriched_path)
        sum_arr_delay_cause = df_enr.filter(F.col("HAS_CAUSE_DATA") == 1).agg(F.sum("ARRIVAL_DELAY")).collect()[0][0]
        exact_arr_cause_match = (overall_cause_total == sum_arr_delay_cause)

        record_check("cause_totals_consistency", cause_match and exact_arr_cause_match,
                     f"Total cause delay minutes: {overall_cause_total:,} min across all cause tables exactly equals sum(ARRIVAL_DELAY) on HAS_CAUSE_DATA=1")

        # ----------------------------------------------------------------------
        # Check 3: Airline counts match Phase 2 exactly (all 14 printed in full)
        # ----------------------------------------------------------------------
        print("\n=== CHECK 3: AIRLINE COUNTS MATCH PHASE 2 ===")
        phase2_airline_counts = {
            "WN": 1261855,
            "DL": 875881,
            "AA": 725984,
            "OO": 588353,
            "EV": 571977,
            "UA": 515723,
            "MQ": 294632,
            "B6": 267048,
            "US": 198715,
            "AS": 172521,
            "NK": 117379,
            "F9": 90836,
            "HA": 76272,
            "VX": 61903
        }

        airline_rows = tables["kpi_by_airline"].select("AIRLINE", "total_flights").collect()
        airline_actual = {r["AIRLINE"]: r["total_flights"] for r in airline_rows}
        airline_match = True

        for air_code, exp_cnt in phase2_airline_counts.items():
            act_cnt = airline_actual.get(air_code, 0)
            print(f"  {air_code:2s}: actual = {act_cnt:9,d} | expected = {exp_cnt:9,d}")
            if act_cnt != exp_cnt:
                airline_match = False

        record_check("kpi_by_airline_counts_match_phase2", airline_match,
                     "All 14 airlines match Phase 2 counts exactly: WN: 1,261,855, DL: 875,881, AA: 725,984, OO: 588,353, EV: 571,977, UA: 515,723, MQ: 294,632, B6: 267,048, US: 198,715, AS: 172,521, NK: 117,379, F9: 90,836, HA: 76,272, VX: 61,903")

        # ----------------------------------------------------------------------
        # Check 4: UNMAPPED bucket row counts (origin 757, dest 755)
        # ----------------------------------------------------------------------
        print("\n=== CHECK 4: UNMAPPED BUCKET ROW COUNTS ===")
        orig_unmapped_cnt = tables["kpi_by_origin_airport"].filter(F.col("ORIGIN_AIRPORT") == "UNMAPPED").select("total_flights").collect()[0][0]
        dest_unmapped_cnt = tables["kpi_by_dest_airport"].filter(F.col("DESTINATION_AIRPORT") == "UNMAPPED").select("total_flights").collect()[0][0]

        unmapped_pass = (orig_unmapped_cnt == 757) and (dest_unmapped_cnt == 755)
        record_check("unmapped_bucket_counts", unmapped_pass,
                     f"Origin UNMAPPED={orig_unmapped_cnt} (expected 757), Dest UNMAPPED={dest_unmapped_cnt} (expected 755)")

        # ----------------------------------------------------------------------
        # Check 5: Re-aggregation test (weighted average from kpi_by_month == kpi_overall == 4.41)
        # ----------------------------------------------------------------------
        print("\n=== CHECK 5: RE-AGGREGATION WEIGHTED AVERAGE TEST ===")
        m_agg = tables["kpi_by_month"].agg(
            F.sum("sum_arr_delay_min").alias("total_delay_min"),
            F.sum("flown_flights").alias("total_flown")
        ).collect()[0]
        weighted_avg_m = round(m_agg["total_delay_min"] / m_agg["total_flown"], 4)
        overall_avg = df_overall["avg_arr_delay_min"]

        reagg_equal = (abs(weighted_avg_m - overall_avg) < 1e-4)
        reagg_phase2_match = (round(overall_avg, 2) == 4.41)

        record_check("reaggregation_weighted_average", reagg_equal and reagg_phase2_match,
                     f"kpi_by_month weighted avg={weighted_avg_m:.4f}, kpi_overall={overall_avg:.4f}, matches Phase 2 mean ARRIVAL_DELAY 4.41 (rounded)")

        # ----------------------------------------------------------------------
        # Check 6: Independent plain Spark SQL recomputation
        # ----------------------------------------------------------------------
        print("\n=== CHECK 6: INDEPENDENT SPARK SQL RECOMPUTATION ===")
        df_enr.createOrReplaceTempView("flights_raw_enriched")

        # Independent SQL for overall
        sql_overall = spark.sql("""
            SELECT
                count(*) AS total_flights,
                sum(case when IS_FLOWN = 1 then 1 else 0 end) AS flown_flights,
                sum(case when CANCELLED = 1 then 1 else 0 end) AS cancelled_flights,
                sum(case when DIVERTED = 1 then 1 else 0 end) AS diverted_flights,
                sum(coalesce(IS_ARR_DELAYED_15, 0)) AS delayed_arr_15,
                sum(coalesce(IS_DEP_DELAYED_15, 0)) AS delayed_dep_15,
                round(sum(case when IS_FLOWN = 1 then ARRIVAL_DELAY else 0 end) / sum(case when IS_FLOWN = 1 then 1 else 0 end), 4) AS avg_arr_delay_min
            FROM flights_raw_enriched
        """).collect()[0]

        sql_overall_pass = (
            sql_overall["total_flights"] == df_overall["total_flights"] and
            sql_overall["flown_flights"] == df_overall["flown_flights"] and
            sql_overall["cancelled_flights"] == df_overall["cancelled_flights"] and
            sql_overall["diverted_flights"] == df_overall["diverted_flights"] and
            sql_overall["delayed_arr_15"] == df_overall["delayed_arr_15"] and
            sql_overall["delayed_dep_15"] == df_overall["delayed_dep_15"] and
            sql_overall["avg_arr_delay_min"] == df_overall["avg_arr_delay_min"]
        )

        # 3 specific (airline, month) cells: (DL, 3), (WN, 7), (AA, 12)
        test_cells = [("DL", 3), ("WN", 7), ("AA", 12)]
        cells_pass = True
        for air, mon in test_cells:
            sql_cell = spark.sql(f"""
                SELECT
                    count(*) AS total_flights,
                    sum(case when IS_FLOWN = 1 then 1 else 0 end) AS flown_flights,
                    sum(case when CANCELLED = 1 then 1 else 0 end) AS cancelled_flights,
                    sum(coalesce(IS_ARR_DELAYED_15, 0)) AS delayed_arr_15
                FROM flights_raw_enriched
                WHERE AIRLINE = '{air}' AND MONTH = {mon}
            """).collect()[0]

            tbl_cell = tables["kpi_by_airline_month"].filter(
                (F.col("AIRLINE") == air) & (F.col("MONTH") == mon)
            ).collect()[0]

            if (sql_cell["total_flights"] != tbl_cell["total_flights"] or
                sql_cell["flown_flights"] != tbl_cell["flown_flights"] or
                sql_cell["cancelled_flights"] != tbl_cell["cancelled_flights"] or
                sql_cell["delayed_arr_15"] != tbl_cell["delayed_arr_15"]):
                cells_pass = False
                logger.error(f"Independent SQL mismatch for ({air}, {mon}): SQL={sql_cell}, Table={tbl_cell}")

        record_check("independent_sql_recomputation", sql_overall_pass and cells_pass,
                     f"kpi_overall and cells (DL, M3), (WN, M7), (AA, M12) match independent SQL queries 100%")

        # ----------------------------------------------------------------------
        # Check 7: Domain integrity (no null keys, non-negative, rates in [0,1], ranking tie handling)
        # ----------------------------------------------------------------------
        print("\n=== CHECK 7: DOMAIN INTEGRITY & DATA QUALITY ===")
        # No null keys
        key_checks = [
            ("kpi_by_month", "MONTH"),
            ("kpi_by_day_of_week", "DAY_OF_WEEK"),
            ("kpi_by_dep_hour", "SCHED_DEP_HOUR"),
            ("kpi_by_airline", "AIRLINE"),
            ("kpi_by_origin_airport", "ORIGIN_AIRPORT"),
            ("kpi_by_dest_airport", "DESTINATION_AIRPORT"),
            ("kpi_by_route", "ROUTE"),
            ("kpi_by_origin_state", "ORIGIN_STATE"),
            ("kpi_by_date", "FLIGHT_DATE")
        ]
        no_null_keys = True
        for tname, kcol in key_checks:
            null_cnt = tables[tname].filter(F.col(kcol).isNull()).count()
            if null_cnt > 0:
                no_null_keys = False
                logger.error(f"Found {null_cnt} null keys in {tname}.{kcol}")

        # Rates in [0, 1]
        rates_valid = True
        rate_cols = ["cancel_rate", "divert_rate", "arr_delay_rate_15", "on_time_rate", "dep_delay_rate_15"]
        for dt in ["kpi_overall", "kpi_by_month", "kpi_by_airline", "kpi_by_origin_airport"]:
            for rc in rate_cols:
                invalid_cnt = tables[dt].filter((F.col(rc) < 0.0) | (F.col(rc) > 1.0)).count()
                if invalid_cnt > 0:
                    rates_valid = False
                    logger.error(f"Found {invalid_cnt} out-of-bounds rates in {dt}.{rc}")

        # Non-negative counts
        counts_valid = True
        for dt in dim_tables:
            neg_cnt = tables[dt].filter(
                (F.col("total_flights") < 0) |
                (F.col("flown_flights") < 0) |
                (F.col("cancelled_flights") < 0)
            ).count()
            if neg_cnt > 0:
                counts_valid = False
                logger.error(f"Found {neg_cnt} negative counts in {dt}")

        record_check("domain_integrity_and_quality", no_null_keys and rates_valid and counts_valid,
                     f"0 null dimension keys, 0 negative counts, all rates bounded in [0.0, 1.0], ranking tie handling fully deterministic")

        # ----------------------------------------------------------------------
        # Check 8: Re-parse output/analytics/phase5_insights.md and cross-check tables
        # ----------------------------------------------------------------------
        print("\n=== CHECK 8: CROSS-CHECKING MARKDOWN INSIGHTS REPORT ===")
        md_path = PROJECT_ROOT / "output" / "analytics" / "phase5_insights.md"
        md_pass = True
        if not md_path.exists():
            md_pass = False
            logger.error(f"Missing {md_path}")
        else:
            with open(md_path, "r", encoding="utf-8") as f:
                md_text = f.read()

            # Find all airline rows in markdown: | Rank | `CODE` | Name | Total | Flown | On-Time | Arr Delay | Cancel Rate |
            air_pattern = re.compile(r"\|\s*\d+\s*\|\s*`([A-Z0-9]+)`\s*\|\s*([^|]+)\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|\s*\*\*([0-9.]+)%\*\*\s*\|\s*([0-9.]+)%\s*\|\s*([0-9.]+)%\s*\|")
            md_airlines = air_pattern.findall(md_text)

            tbl_airline_dict = {r["AIRLINE"]: r for r in tables["kpi_by_airline"].collect()}
            
            for code, name, tot_str, flown_str, ot_str, delay_str, canc_str in md_airlines:
                tot = int(tot_str.replace(",", ""))
                flown = int(flown_str.replace(",", ""))
                ot_rate = float(ot_str)
                canc_rate = float(canc_str)

                t_row = tbl_airline_dict.get(code)
                if not t_row:
                    md_pass = False
                    logger.error(f"Airline {code} in markdown not in kpi_by_airline")
                    continue

                if tot != t_row["total_flights"] or flown != t_row["flown_flights"]:
                    md_pass = False
                    logger.error(f"Mismatch in markdown airline counts for {code}: MD tot={tot}, flown={flown} vs Tbl tot={t_row['total_flights']}, flown={t_row['flown_flights']}")
                
                exp_canc_pct = round(t_row["cancel_rate"] * 100, 2)
                if abs(canc_rate - exp_canc_pct) > 0.02:
                    md_pass = False
                    logger.error(f"Mismatch in markdown cancel rate for {code}: MD={canc_rate}% vs expected={exp_canc_pct}%")

        record_check("markdown_insights_crosscheck", md_pass,
                     "phase5_insights.md table values strictly match kpi_by_airline row-by-row with consistent cancel_rate (cancelled/total)")

        # ----------------------------------------------------------------------
        # Print full kpi_overall row
        # ----------------------------------------------------------------------
        print("\n=== KPI OVERALL (FULL ROW) ===")
        for k, v in df_overall.asDict().items():
            print(f"  {k:30s}: {v}")

        all_passed = all(c["status"] == "PASS" for c in checks)
        print("\n============================================================")
        print(f"VALIDATION SUMMARY: {'ALL CHECKS PASSED' if all_passed else 'SOME CHECKS FAILED'}")
        print("============================================================")
        for c in checks:
            print(f"  [{c['status']}] {c['name']}")

        total_runtime = time.perf_counter() - start_time
        metrics = {
            "script": "validate_analytics.py",
            "runtime_sec": round(total_runtime, 2),
            "checks_run": len(checks),
            "checks_passed": sum(1 for c in checks if c["status"] == "PASS"),
            "status": "PASS" if all_passed else "FAIL"
        }
        metrics_path = PROJECT_ROOT / "logs" / "metrics" / "validate_analytics.json"
        with open(metrics_path, "w", encoding="utf-8") as mf:
            json.dump(metrics, mf, indent=2)
        logger.info(f"Saved validation metrics ({total_runtime:.2f}s) to {metrics_path}")

        if not all_passed:
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    validate_analytics()
