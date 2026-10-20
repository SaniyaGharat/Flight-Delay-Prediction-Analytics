import sys
import os
import time
import json
import yaml
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, StringType
from src.utils.spark_session import get_spark_session
from src.ml.guard import validate_feature_selection

def main():
    start_time = time.perf_counter()
    os.makedirs("logs/metrics", exist_ok=True)
    
    # 1. Load config
    with open("config/ml_features.yaml") as f:
        ml_cfg = yaml.safe_load(f)
        
    cat_features = ml_cfg["features"]["categorical"]
    num_features = ml_cfg["features"]["numeric"]
    label_col = ml_cfg["label"]
    
    # Validate selected features
    validate_feature_selection(cat_features + num_features)
    print("Pre-departure feature selection passed validation guard.")
    
    spark = get_spark_session("build_ml_dataset")
    
    # 2. Read flights_enriched
    enriched_path = "hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_enriched"
    df = spark.read.parquet(enriched_path).filter(F.col("IS_FLOWN") == 1)
    
    total_flown = df.count()
    print(f"Total flown flights read: {total_flown:,}")
    assert total_flown == 5714008, f"Expected 5,714,008 rows, got {total_flown:,}"
    
    # 3. Assert against kpi_by_month
    kpi_df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/analytics/kpi_by_month")
    kpi_counts = {row["month"]: row["flown_flights"] for row in kpi_df.select("month", "flown_flights").collect()}
    
    monthly_counts = {row["MONTH"]: row["count"] for row in df.groupBy("MONTH").count().collect()}
    
    print("\nVerifying monthly flown counts against kpi_by_month:")
    for m in range(1, 13):
        kpi_cnt = kpi_counts.get(m, 0)
        df_cnt = monthly_counts.get(m, 0)
        assert df_cnt == kpi_cnt, f"Month {m} count mismatch: df={df_cnt}, kpi={kpi_cnt}"
        print(f"  Month {m:2d}: {df_cnt:,} (matches kpi_by_month)")
    print("All 12 monthly counts match kpi_by_month.flown_flights exactly!")
    
    # 4. Handle Nulls in features
    # ORIGIN_AIRPORT / DESTINATION_AIRPORT -> 'UNMAPPED'
    df = (df.withColumn("ORIGIN_AIRPORT", F.coalesce(F.col("ORIGIN_AIRPORT"), F.lit("UNMAPPED")))
            .withColumn("DESTINATION_AIRPORT", F.coalesce(F.col("DESTINATION_AIRPORT"), F.lit("UNMAPPED")))
            .withColumn("DAY_OF_WEEK", F.col("DAY_OF_WEEK").cast(StringType()))
            .withColumn("label", F.col(label_col).cast(DoubleType())))
           
    # 5. Split definition
    train_raw = df.filter(F.col("MONTH") <= 9)
    val_raw = df.filter(F.col("MONTH") == 10)
    test_raw = df.filter(F.col("MONTH") >= 11)
    
    # Compute TRAIN median for SCHEDULED_TIME
    train_sched_median = float(train_raw.approxQuantile("SCHEDULED_TIME", [0.5], 0.001)[0])
    print(f"\nComputed TRAIN median for SCHEDULED_TIME: {train_sched_median}")
    
    # Impute SCHEDULED_TIME with TRAIN median across all splits
    train_df = train_raw.withColumn("SCHEDULED_TIME", F.coalesce(F.col("SCHEDULED_TIME"), F.lit(train_sched_median)))
    val_df = val_raw.withColumn("SCHEDULED_TIME", F.coalesce(F.col("SCHEDULED_TIME"), F.lit(train_sched_median)))
    test_df = test_raw.withColumn("SCHEDULED_TIME", F.coalesce(F.col("SCHEDULED_TIME"), F.lit(train_sched_median)))
    
    # 6. Selected columns to persist for ML
    persist_cols = ["FLIGHT_DATE", "MONTH", "label"] + cat_features + num_features
    train_out = train_df.select(persist_cols)
    val_out = val_df.select(persist_cols)
    test_out = test_df.select(persist_cols)
    
    # 7. Print Split Distribution & Positive Rates
    print("\n" + "="*80)
    print("CHRONOLOGICAL DATASET SPLIT SUMMARY")
    print("="*80)
    print(f"{'Split':<15} | {'Months':<12} | {'Total Rows':<12} | {'Delayed (15m+)':<15} | {'Delay Rate (%)':<15}")
    print("-" * 80)
    
    splits_summary = {}
    for name, s_df, months_str in [("Train", train_out, "1-9 (Jan-Sep)"), ("Validation", val_out, "10 (Oct)"), ("Test", test_out, "11-12 (Nov-Dec)")]:
        cnt = s_df.count()
        pos = s_df.filter(F.col("label") == 1.0).count()
        rate = (pos / cnt) * 100 if cnt > 0 else 0.0
        splits_summary[name] = {"rows": cnt, "positive": pos, "rate_pct": round(rate, 2), "months": months_str}
        print(f"{name:<15} | {months_str:<12} | {cnt:<12,} | {pos:<15,} | {rate:14.2f}%")
    print("-" * 80)
    print(f"Total Rows Check: {sum(v['rows'] for v in splits_summary.values()):,} (Target: 5,714,008)")
    print("Note on Base Rate Drift: October validation base rate drops to ~12.44% (annual low), then rises to ~15.26% in Nov and ~20.60% in Dec.")
    
    # 8. Persist to HDFS
    base_hdfs = "hdfs://localhost:9000/user/saniya/flight_delay/processed/ml/dataset"
    print(f"\nPersisting clean ML splits to {base_hdfs}...")
    train_out.write.mode("overwrite").parquet(f"{base_hdfs}/train")
    val_out.write.mode("overwrite").parquet(f"{base_hdfs}/val")
    test_out.write.mode("overwrite").parquet(f"{base_hdfs}/test")
    print("Saved train, val, and test partitions to HDFS.")
    
    duration = time.perf_counter() - start_time
    metrics = {
        "script": "build_ml_dataset.py",
        "duration_seconds": round(duration, 2),
        "total_rows": total_flown,
        "splits": splits_summary,
        "train_sched_time_median": train_sched_median
    }
    with open("logs/metrics/build_ml_dataset.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nExecution finished in {duration:.2f} seconds. Metrics logged to logs/metrics/build_ml_dataset.json")
    
    spark.stop()

if __name__ == "__main__":
    main()
