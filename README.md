# Flight Delay Prediction and Analytics

An end-to-end Big Data analytics and machine learning pipeline for flight delay prediction and historical performance analytics using **VS Code**, **HDFS**, **Apache Spark / PySpark**, and **Power BI**.

---

## 📁 Project Architecture & Directory Structure

```text
flight-delay-analytics/
├── .vscode/                  # VS Code interpreter settings and debug profiles
│   ├── settings.json
│   └── launch.json
├── config/
│   ├── config.yaml           # Centralized configuration (HDFS paths, Spark tuning, local raw files)
│   └── hadoop/               # Hadoop XML configs (core-site.xml, hdfs-site.xml, setup scripts)
├── data/
│   └── raw/                  # Local copies of raw flight CSVs (gitignored)
│       ├── airlines.csv
│       ├── airports.csv
│       └── flights.csv
├── logs/                     # Ingestion and application logs (gitignored)
│   └── ingestion.log
├── notebooks/                # Jupyter / exploratory analysis notebooks
├── output/
│   └── powerbi/              # Aggregated analytical datasets exported for Power BI (gitignored)
├── scripts/                  # Cluster and environment lifecycle scripts
│   ├── start_hdfs.sh         # Idempotent HDFS startup and healthcheck script
│   └── stop_hdfs.sh          # Idempotent HDFS shutdown script
├── src/
│   ├── __init__.py
│   ├── ingestion/            # Raw data ingestion into HDFS and schema validation
│   │   ├── __init__.py
│   │   ├── ingest_to_hdfs.py # Uploads raw CSVs to HDFS with byte integrity verification
│   │   └── validate_raw.py   # PySpark validation of raw HDFS datasets
│   ├── processing/           # Data cleaning, transformation, and Parquet staging (Phase 3+)
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

- **Execution Engine**: Apache Spark (PySpark 3.5.x)
- **Distributed Storage**: Apache Hadoop HDFS 3.3.6 (Single-node pseudo-distributed mode)
- **Runtime Environment**: WSL2 (Ubuntu 22.04 LTS with native `systemd` enabled)
- **Programming Language**: Python 3.11.x (WSL virtual environment)
- **Java Virtual Machine**: OpenJDK 17 LTS (`/usr/lib/jvm/java-17-openjdk-amd64`)
- **Reporting & BI**: Microsoft Power BI Desktop
- **IDE**: Visual Studio Code (WSL Remote Extension)

---

## 🖥️ How to Start the Environment

All development and cluster execution runs inside **WSL2** (`Ubuntu-22.04`).

### 1. Launch WSL and Open in VS Code
From PowerShell on Windows or directly in a terminal:
```powershell
wsl
```
Inside the WSL terminal, navigate to the project directory and open VS Code:
```bash
cd ~/flight-delay-analytics
code .
```
*(This launches VS Code connected directly to the WSL Ubuntu environment via the WSL Remote Extension).*

### 2. Start HDFS Daemons (One-Line Command After Every WSL Restart)
Run the automated, idempotent startup script:
```bash
~/flight-delay-analytics/scripts/start_hdfs.sh
```
This script automatically:
1. Starts the SSH service (`sudo service ssh start`).
2. Starts HDFS daemons (`start-dfs.sh`) only if not already running.
3. Polls NameNode safe mode until `hdfs dfsadmin -safemode get` reports `Safe mode is OFF`.
4. Displays active Java processes via `jps` (NameNode, DataNode, SecondaryNameNode).
5. Lists the base HDFS directory: `hdfs dfs -ls /user/$USER/flight_delay`.

### 3. Stop HDFS Daemons
To safely and idempotently stop all HDFS daemons:
```bash
~/flight-delay-analytics/scripts/stop_hdfs.sh
```

### 4. Web UIs
- **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)
- **Spark Application UI**: [http://localhost:4040](http://localhost:4040) *(active while a SparkContext is running)*

---

## 📥 Phase 1: Raw Data Ingestion & Validation

Phase 1 establishes the single-node pseudo-distributed HDFS foundation and ingests the raw Kaggle 2015 Flight Delays dataset into dedicated HDFS subfolders with strict replication factor 1 and byte-level integrity verification.

### 1. Raw Datasets Overview

Local raw data files reside in `data/raw/`:
- `airlines.csv`: 14 airlines (15 lines including header, 359 bytes, 2 columns)
- `airports.csv`: 322 airports (323 lines including header, 23,867 bytes, 7 columns)
- `flights.csv`: 5,819,079 flight records (5,819,080 lines including header, 592,406,591 bytes, 31 columns)

### 2. HDFS Directory Structure
Raw files are uploaded to dedicated subfolders in HDFS:
```text
/user/saniya/flight_delay/raw/
├── airlines/
│   └── airlines.csv
├── airports/
│   └── airports.csv
└── flights/
    └── flights.csv
```

### 3. Upload Raw Data to HDFS (`ingest_to_hdfs.py`)
Run the ingestion script within the WSL virtual environment:
```bash
cd ~/flight-delay-analytics
.venv/bin/python src/ingestion/ingest_to_hdfs.py
```
**Key Features:**
- Reads paths dynamically from `config/config.yaml` (no hardcoded paths).
- Idempotent upload via `hdfs dfs -put -f` into dedicated subfolders.
- Performs byte-by-byte comparison between local file sizes and HDFS file sizes via `hdfs dfs -stat %b`.
- Logs all operations to `logs/ingestion.log` and console.
- **Verification Mode**: Use `--verify-only` to check file existence and byte sizes without re-uploading:
  ```bash
  .venv/bin/python src/ingestion/ingest_to_hdfs.py --verify-only
  ```

### 4. Validate Ingested Data with PySpark (`validate_raw.py`)
Run the PySpark validation script:
```bash
.venv/bin/python src/ingestion/validate_raw.py
```
**Validation Criteria:**
- Reads directly from HDFS URIs with `header=True` and `inferSchema=False` (preserving all 31 columns as strings).
- Displays schema and first 5 sample rows for each dataset.
- Verifies exact record counts match `(wc -l minus 1)` from the local source files:
  - `airlines`: 14 rows, 2 columns
  - `airports`: 322 rows, 7 columns
  - `flights`: 5,819,079 rows, 31 columns
- Does NOT perform any cleaning, casting, or transformations (deferred to Phase 3+).

### 5. Verification Commands
Inspect HDFS storage and replication factor (configured to 1):
```bash
# Recursive listing showing replication factor (column 2)
hdfs dfs -ls -R /user/saniya/flight_delay/raw

# Disk usage per subfolder
hdfs dfs -du -h /user/saniya/flight_delay/raw

# Health check, block layout, and replication factor on flights.csv
hdfs fsck /user/saniya/flight_delay/raw/flights/flights.csv -files -blocks
```
