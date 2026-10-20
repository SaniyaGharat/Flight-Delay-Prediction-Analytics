"""
scripts/verify/validate_feature_roles.py - Verifies config/feature_roles.yaml against HDFS flights_enriched schema.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path("/home/saniya/flight-delay-analytics")
sys.path.insert(0, str(PROJECT_ROOT))

import yaml
from src.utils.spark_session import get_spark_session

def validate_roles():
    print("=" * 80)
    print("VERIFYING config/feature_roles.yaml AGAINST HDFS flights_enriched SCHEMA")
    print("=" * 80)
    
    # 1. Read YAML
    yaml_path = PROJECT_ROOT / "config" / "feature_roles.yaml"
    with open(yaml_path, "r", encoding="utf-8") as f:
        meta = yaml.safe_load(f)
    
    yaml_cols_dict = meta.get("columns", {})
    yaml_col_names = set(yaml_cols_dict.keys())
    declared_total = meta.get("total_columns")
    
    print(f"Declared total_columns in YAML: {declared_total}")
    print(f"Actual columns defined in YAML: {len(yaml_col_names)}")
    
    # 2. Read Real Parquet Schema
    spark = get_spark_session("validate_feature_roles")
    df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_enriched")
    
    print("\n--- Real Parquet printSchema() ---")
    df.printSchema()
    
    parquet_col_names = set(df.columns)
    parquet_total = len(df.columns)
    print(f"Parquet Column Count: {parquet_total}")
    
    # 3. Assertions
    missing_in_yaml = parquet_col_names - yaml_col_names
    extra_in_yaml = yaml_col_names - parquet_col_names
    
    print("\n--- Schema vs YAML Comparison ---")
    print(f"Columns in Parquet: {parquet_total}")
    print(f"Columns in YAML:    {len(yaml_col_names)}")
    print(f"Missing in YAML:    {missing_in_yaml if missing_in_yaml else 'None (0)'}")
    print(f"Extra in YAML:      {extra_in_yaml if extra_in_yaml else 'None (0)'}")
    
    assert declared_total == parquet_total, f"Declared total_columns ({declared_total}) != Parquet column count ({parquet_total})"
    assert len(yaml_col_names) == parquet_total, f"YAML columns count ({len(yaml_col_names)}) != Parquet count ({parquet_total})"
    assert len(missing_in_yaml) == 0, f"Columns missing in YAML: {missing_in_yaml}"
    assert len(extra_in_yaml) == 0, f"Extra columns in YAML: {extra_in_yaml}"
    
    # 4. Check model_use presence and role rules
    allowed_roles = {"identifier", "dimension", "pre-departure feature candidate", "post-event fact", "label", "flag"}
    allowed_uses = {"identifier", "categorical-feature", "numeric-feature", "label", "excluded-leakage", "audit"}
    
    for col_name, info in yaml_cols_dict.items():
        role = info.get("role")
        use = info.get("model_use")
        assert role in allowed_roles, f"Invalid role '{role}' for column {col_name}"
        assert use in allowed_uses, f"Invalid model_use '{use}' for column {col_name}"
        if use == "excluded-leakage":
            assert "leakage_justification" in info, f"Missing leakage_justification for {col_name}"
    
    # Check specific requested mappings
    assert yaml_cols_dict["AIRLINE"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["ORIGIN_AIRPORT"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["DESTINATION_AIRPORT"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["ROUTE"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["DAY_OF_WEEK"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["MONTH"]["model_use"] == "categorical-feature"
    assert yaml_cols_dict["HAS_CAUSE_DATA"]["role"] == "flag"
    assert yaml_cols_dict["HAS_CAUSE_DATA"]["model_use"] == "excluded-leakage"
    assert yaml_cols_dict["IS_FLOWN"]["role"] == "flag"
    assert yaml_cols_dict["IS_FLOWN"]["model_use"] == "excluded-leakage"
    
    print("\n[SUCCESS] All assertions PASSED! YAML column set strictly equals Parquet schema (70 columns).")
    spark.stop()

if __name__ == "__main__":
    validate_roles()
