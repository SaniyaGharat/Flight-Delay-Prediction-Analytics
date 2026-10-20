# Flight Delay Prediction and Analytics

An end-to-end Big Data analytics and machine learning pipeline for flight delay prediction and historical performance analytics using **VS Code**, **HDFS**, **Apache Spark / PySpark**, and **Power BI**.

---

## 📁 Project Architecture & Skeleton

```text
flight-delay-analytics/
├── .vscode/                  # VS Code interpreter settings and debug profiles
│   ├── settings.json
│   └── launch.json
├── config/
│   ├── config.yaml           # Centralized configuration (HDFS URIs, Spark tuning, local paths)
│   └── hadoop/               # Hadoop core-site.xml and hdfs-site.xml configurations
├── data/
│   └── raw/                  # Local copies of input flight datasets (gitignored)
├── logs/                     # Application, pipeline, and verification logs (gitignored)
├── notebooks/                # Jupyter / exploratory analysis notebooks
├── output/
│   ├── profiling/            # Phase 2 raw data profiling markdown report
│   ├── cleaning/             # Phase 3 cleaning report & airport_id_map CSV
│   └── powerbi/              # Aggregated analytical datasets exported for Power BI (gitignored)
├── scripts/                  # Shell utility scripts (start_hdfs.sh, stop_hdfs.sh, verify_*.sh)
├── src/
│   ├── __init__.py
│   ├── ingestion/            # Raw data ingestion into HDFS (ingest_to_hdfs.py)
│   ├── profiling/            # Raw data profiling pipeline (profile_raw.py)
│   ├── processing/           # Airport ID mapping, data cleaning, and validation
│   │   ├── build_airport_map.py
│   │   ├── clean_flights.py
│   │   └── validate_clean.py
│   ├── analytics/            # SQL-based analytics and delay metrics aggregation
│   ├── ml/                   # Delay prediction feature engineering and MLlib models
│   ├── export/               # CSV/Parquet export pipeline for Power BI consumption
│   └── utils/
│       ├── __init__.py
│       ├── spark_session.py  # Reusable SparkSession builder with HDFS & resource tuning
│       └── test_setup.py     # End-to-end smoke test script
├── .gitignore
├── README.md
└── requirements.txt          # Python dependencies (PySpark, Pandas, PyArrow, etc.)
```

---

## 🛠️ Technology Stack & Versions

- **Execution Engine**: Apache Spark (PySpark 3.5.3)
- **Distributed Storage**: Apache Hadoop HDFS 3.3.6 (pseudo-distributed mode, replication factor 1)
- **Environment**: Ubuntu 22.04 LTS on WSL2 (Windows Subsystem for Linux)
- **Python**: Python 3.11.17 (dedicated virtual environment `.venv`)
- **Java Virtual Machine**: OpenJDK 17 (`/usr/lib/jvm/java-17-openjdk-amd64`)
- **Compression & Formats**: Apache Parquet with Snappy compression
- **Reporting & BI**: Microsoft Power BI Desktop
- **IDE**: Visual Studio Code

---

## ⚠️ Important Note on WSL2 Daemon Persistence

In WSL2, background daemons (including HDFS NameNode, DataNode, and SecondaryNameNode) are bound to the lifecycle of active WSL sessions. When no interactive terminal or background session remains open, WSL will terminate background processes upon session exit (`SIGTERM`/`SIGHUP`).

To guarantee uninterrupted operation:
1. Use `scripts/start_hdfs.sh` to launch and verify all HDFS daemons before executing any pipeline step.
2. The scripts are idempotent and will safely reuse existing daemons if already active.
3. Use `scripts/stop_hdfs.sh` for graceful daemon shutdown.

---

## 🚀 Execution & Pipeline Phases

### Phase 1: Ingestion into HDFS
- Raw CSV files (`flights.csv`, `airports.csv`, `airlines.csv`) ingested directly into HDFS at `/user/saniya/flight_delay/raw/`.
- Configured HDFS default replication factor to `1` in `hdfs-site.xml` and `spark.hadoop.dfs.replication=1`.
- Verified 100% byte-for-byte and SHA256 checksum equality between local CSV files and HDFS replicas via `src/ingestion/ingest_to_hdfs.py --verify-only`.

### Phase 2: Raw Data Profiling
- Executed `src/profiling/profile_raw.py` on 5,819,079 raw flight records.
- Documented complete schema distributions, null counts, monthly/daily distributions, delay cause missingness (81.72% null for non-delayed flights), and natural key duplicate candidates.
- Generated profiling artifacts in `output/profiling/phase2_profile.md`.

### Phase 3: Data Cleaning & Parquet Transformation
Phase 3 establishes an enterprise-grade cleaned data layer in HDFS `processed/`:
1. **Airport ID Mapping (`src/processing/build_airport_map.py`)**:
   - Reconstructs missing 3-letter IATA airport codes for October 2015 (which exclusively recorded 5-digit DOT numeric IDs) by matching repeating flight schedules across non-October months.
   - Successfully mapped 296 distinct airport IDs (96.4% coverage; 0 ambiguous; 11 unmapped affecting 1,450 October flights).
   - Validated against 7 standard DOT anchor airports (ATL, ORD, DFW, JFK, LAX, EWR, SFO) with 100% agreement.
   - Saved mapping table to `processed/airport_id_map` (Parquet) and `output/cleaning/airport_id_map.csv`.

2. **Flights & Lookups Cleaning (`src/processing/clean_flights.py`)**:
   - String trimming, empty string normalization to `null`.
   - Normalization of `"2400"` time values to `"0000"` across 6 time columns; tagged with `HAS_2400_TIME`.
   - Strict type casting of 20 columns to `IntegerType` with asserted zero-loss.
   - Date derivation (`FLIGHT_DATE`) with 100% day-of-week verification.
   - Operational status flags (`IS_FLOWN`, `HAS_CAUSE_DATA`, `CANCELLATION_REASON_DESC`).
   - Deduplication: Repartitioned by `MONTH` before `dropDuplicates()` (0 exact duplicate rows dropped; 5,819,079 rows retained).
   - Flagged 1 natural-key duplicate pair (2 rows) with `DUP_NATURAL_KEY=1`.
   - Saved Snappy Parquet tables to HDFS:
     - `processed/flights_clean` (partitioned by `MONTH`, 152.9 MB, 74.2% compression ratio)
     - `processed/airports_clean` (322 rows, 17.9 KB)
     - `processed/airlines_clean` (14 rows, 1.0 KB)

3. **Validation Suite (`src/processing/validate_clean.py`)**:
   - Automated test suite asserting all 8 integrity checks (row counts, monthly counts, null counts, flag counts, IATA referential integrity, and time normalization).
   - Result: **8 / 8 Checks PASSED**.
