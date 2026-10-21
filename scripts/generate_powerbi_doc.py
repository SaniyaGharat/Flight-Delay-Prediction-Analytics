#!/usr/bin/env python3
"""
generate_powerbi_doc.py

Generates output/powerbi/MODEL.md containing:
- Star schema architecture & relationships
- Role-playing airport dimensions documentation
- Full Data Dictionary generated dynamically from actual Parquet schemas
"""

import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def load_config():
    cfg_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main():
    config = load_config()
    processed_dir = config.get("hdfs", {}).get("processed_dir", "/user/saniya/flight_delay/processed")
    if not processed_dir.startswith("hdfs://"):
        processed_dir = f"hdfs://localhost:9000{processed_dir}"
    powerbi_hdfs_base = f"{processed_dir}/powerbi"
    
    spark = SparkSession.builder \
        .appName("gen_powerbi_doc") \
        .master("local[2]") \
        .getOrCreate()
        
    tables = [
        ("dim_date", "Dimension: Calendar Date", "date"),
        ("dim_airline", "Dimension: Commercial Air Carrier", "airline"),
        ("dim_airport", "Dimension: Commercial Airport (Role-Playing)", "airport"),
        ("fact_daily_airline", "Fact: Daily Carrier Performance", "fact"),
        ("fact_daily_origin", "Fact: Daily Origin Airport Operations", "fact"),
        ("fact_daily_dest", "Fact: Daily Destination Airport Arrivals", "fact"),
        ("fact_airline_dow_hour", "Fact: Airline Schedule Profile by DOW & Hour", "fact"),
        ("fact_route_month", "Fact: Monthly Route Delay & Volume Profile", "fact"),
        ("fact_cause_daily_airline", "Fact: Daily Carrier Delay Cause Breakdown (Long)", "fact"),
        ("fact_cancel_daily_airline_reason", "Fact: Daily Carrier Cancellation Reasons (Long)", "fact")
    ]
    
    lines = [
        "# PowerBI Star-Schema Data Model & Architecture Documentation",
        "",
        "## 1. Dimensional Architecture & Star Schema Design",
        "",
        "The PowerBI analytical dataset is structured as a conformed dimensional star schema. To support interactive exploration of 5.82 million flights with sub-second response times in PowerBI Desktop and Service, the raw record-level data has been pre-aggregated into additive, atomic fact tables aligned along conformed business dimensions.",
        "",
        "### Core Design Principles",
        "1. **Strictly Additive Measures**: Fact tables store base integer and double sums (`total_flights`, `flown_flights`, `delayed_arr_15`, `sum_arr_delay_min`). Precomputed ratios are intentionally excluded from facts to ensure correct mathematical rollups across dynamic slicer contexts.",
        "2. **Safe DIVIDE DAX Ratios**: All rates, shares, and averages are defined via DAX measures using `DIVIDE(numerator, denominator, 0)`.",
        "3. **Zero Orphan Foreign Keys**: Airport codes without mappings in October are standardized to `'UNMAPPED'`, which maps to a dedicated dimension entity in `dim_airport`.",
        "4. **Bi-Format Export**: All tables are dual-published as Snappy-compressed Parquet in HDFS (`processed/powerbi/`) and single-file UTF-8 CSVs in `output/powerbi/`.",
        "",
        "---",
        "",
        "## 2. Table Relationships & Role-Playing Dimensions",
        "",
        "| From (Fact Table) | Foreign Key | To (Dimension Table) | Primary Key | Cardinality | Active / Inactive | Operational Relationship |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        "| `fact_daily_airline` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight operation date |",
        "| `fact_daily_airline` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Operating air carrier |",
        "| `fact_daily_origin` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight departure date |",
        "| `fact_daily_origin` | `ORIGIN_AIRPORT` | `dim_airport` | `code` | Many-to-One (*:1) | **Active** | Origin departure airport |",
        "| `fact_daily_dest` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight arrival date |",
        "| `fact_daily_dest` | `DEST_AIRPORT` | `dim_airport` | `code` | Many-to-One (*:1) | *Inactive* | Role-playing destination airport (activated via `USERELATIONSHIP`) |",
        "| `fact_airline_dow_hour` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Carrier schedule aggregation |",
        "| `fact_route_month` | `ORIGIN` | `dim_airport` | `code` | Many-to-One (*:1) | **Active** | Route origin endpoint |",
        "| `fact_route_month` | `DEST` | `dim_airport` | `code` | Many-to-One (*:1) | *Inactive* | Route destination endpoint |",
        "| `fact_cause_daily_airline` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Date of delay occurrence |",
        "| `fact_cause_daily_airline` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Attributed carrier |",
        "| `fact_cancel_daily_airline_reason` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Date of cancellation |",
        "| `fact_cancel_daily_airline_reason` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Carrier cancelling flight |",
        "",
        "### Role-Playing Dimension Pattern for `dim_airport`",
        "Because flights have two airport endpoints (Origin and Destination), `dim_airport` functions as a **Role-Playing Dimension**:",
        "- **Origin Airport**: The relationship between `fact_daily_origin[ORIGIN_AIRPORT]` and `dim_airport[code]` is **Active** by default.",
        "- **Destination Airport**: The relationship between `fact_daily_dest[DEST_AIRPORT]` and `dim_airport[code]` is configured as **Inactive** in the model. In DAX measures focusing on arrival airports, the relationship is programmatically activated using `USERELATIONSHIP`:",
        "  ```dax",
        "  Flights by Destination Airport = ",
        "  CALCULATE(",
        "      SUM(fact_daily_dest[total_flights]),",
        "      USERELATIONSHIP(fact_daily_dest[DEST_AIRPORT], dim_airport[code])",
        "  )",
        "  ```",
        "",
        "---",
        "",
        "## 3. Data Dictionary (Generated from Actual Schemas)",
        ""
    ]
    
    for tbl_name, tbl_desc, tbl_cat in tables:
        df = spark.read.parquet(f"{powerbi_hdfs_base}/{tbl_name}")
        row_cnt = df.count()
        
        lines.append(f"### Table: `{tbl_name}`")
        lines.append(f"- **Description**: {tbl_desc}")
        lines.append(f"- **Row Count**: {row_cnt:,}")
        lines.append(f"- **Parquet Storage**: `{powerbi_hdfs_base}/{tbl_name}`")
        lines.append(f"- **Local CSV File**: `output/powerbi/{tbl_name}.csv`")
        lines.append("")
        lines.append("| Column Name | Data Type | Nullable | Description / Business Meaning |")
        lines.append("| :--- | :--- | :--- | :--- |")
        
        for field in df.schema.fields:
            col_name = field.name
            col_type = str(field.dataType).replace("Type()", "").replace("Type", "")
            nullable = "YES" if field.nullable else "NO"
            
            # Contextual description
            desc = "Dimensional key or attribute"
            if col_name in ["DATE", "FLIGHT_DATE"]:
                desc = "ISO calendar date (YYYY-MM-DD), primary date join key"
            elif col_name in ["code", "AIRLINE", "ORIGIN_AIRPORT", "DEST_AIRPORT", "ORIGIN", "DEST"]:
                desc = "Standard IATA code or dimension primary key"
            elif col_name == "name":
                desc = "Full business name of carrier or airport"
            elif col_name == "total_flights":
                desc = "Total scheduled flight operations count"
            elif col_name == "flown_flights":
                desc = "Successfully completed flight count (cancelled=0 and diverted=0)"
            elif col_name == "cancelled_flights":
                desc = "Cancelled flight count"
            elif col_name == "diverted_flights":
                desc = "Diverted flight count"
            elif col_name == "delayed_arr_15":
                desc = "Flights arriving 15+ minutes behind schedule"
            elif col_name == "delayed_dep_15":
                desc = "Flights departing 15+ minutes behind schedule"
            elif col_name == "sum_arr_delay_min":
                desc = "Sum of arrival delay minutes across flown flights"
            elif col_name == "sum_dep_delay_min":
                desc = "Sum of departure delay minutes across scheduled flights"
            elif col_name == "sum_delay_minutes_pos":
                desc = "Sum of positive arrival delay minutes (for severity metrics)"
            elif col_name == "CAUSE":
                desc = "FAA delay cause category (Air System, Security, Airline, Late Aircraft, Weather)"
            elif col_name == "MINUTES":
                desc = "Total attributed delay minutes for specified cause"
            elif col_name == "FLIGHTS":
                desc = "Count of flights experiencing delay in specified cause"
            elif col_name == "CANCELLATION_REASON":
                desc = "FAA cancellation reason code (A=Carrier, B=Weather, C=NAS, D=Security)"
            elif col_name == "CANCELLED_FLIGHTS":
                desc = "Count of cancelled flights under specified cancellation reason"
            elif col_name == "ROUTE":
                desc = "Airport pair identifier formatted as ORIGIN-DEST (e.g., ORD-LAX)"
            elif col_name == "SCHED_DEP_HOUR":
                desc = "Scheduled departure clock hour (0 to 23)"
            elif col_name == "DAY_OF_WEEK":
                desc = "Day of week integer (1=Monday to 7=Sunday)"
            elif col_name == "MONTH":
                desc = "Calendar month integer (1 to 12)"
                
            lines.append(f"| `{col_name}` | `{col_type}` | {nullable} | {desc} |")
        lines.append("")
        
    doc_path = PROJECT_ROOT / "output" / "powerbi" / "MODEL.md"
    doc_path.parent.mkdir(parents=True, exist_ok=True)
    with open(doc_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
        
    print(f"[SUCCESS] PowerBI Model documentation written to {doc_path} ({len(lines)} lines)")
    spark.stop()

if __name__ == "__main__":
    main()
