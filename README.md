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
│   └── config.yaml           # Centralized configuration (HDFS URIs, Spark tuning, local paths)
├── data/
│   └── raw/                  # Local copies of input flight datasets (gitignored)
├── logs/                     # Application and execution logs (gitignored)
├── notebooks/                # Jupyter / exploratory analysis notebooks
├── output/
│   └── powerbi/              # Aggregated analytical datasets exported for Power BI (gitignored)
├── src/
│   ├── __init__.py
│   ├── ingestion/            # Raw data ingestion into HDFS
│   ├── processing/           # Data cleaning, transformation, and Parquet staging
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
- **Distributed Storage**: Hadoop Distributed File System (HDFS 3.3.x / pseudo-distributed mode)
- **Programming Language**: Python 3.10 – 3.12 (recommended for PySpark 3.5.x compatibility)
- **Java Virtual Machine**: Java OpenJDK 11 or 17 (Java 8/11/17 LTS required by Spark)
- **Reporting & BI**: Microsoft Power BI Desktop
- **IDE**: Visual Studio Code

---

## 🚀 Getting Started

### 1. Python Environment Setup

Create and activate a virtual environment:

```powershell
# In PowerShell:
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Upgrade pip and install dependencies:
pip install -r requirements.txt
```

### 2. Hadoop / HDFS Setup (WSL2 Recommended on Windows)

Native Hadoop on Windows requires custom compiled native binaries (`winutils.exe`, `hadoop.dll`) and has known permission/path compatibility issues. Setting up Hadoop pseudo-distributed mode inside **WSL2 (Ubuntu 22.04 LTS)** provides native Linux stability, seamless HDFS daemon management, and full POSIX compliance.

#### Steps to enable WSL2 and Hadoop:
1. Enable WSL2 and install Ubuntu:
   ```powershell
   wsl --install -d Ubuntu-22.04
   ```
2. In Ubuntu, install Java 11 or 17:
   ```bash
   sudo apt update && sudo apt install -y openjdk-11-jdk
   ```
3. Download Hadoop 3.3.6 and configure `core-site.xml` and `hdfs-site.xml`.
4. Format NameNode and start daemons:
   ```bash
   hdfs namenode -format
   start-dfs.sh
   ```
5. Check running services with `jps` and verify the Web UI at:
   - **HDFS NameNode UI**: [http://localhost:9870](http://localhost:9870)

### 3. Verify Setup (Smoke Test)

Run the verification test script:

```powershell
python src/utils/test_setup.py
```

The script will:
1. Initialize the configured `SparkSession`.
2. Connect to the target filesystem (HDFS or local fallback).
3. Generate a sample DataFrame with flight delay records.
4. Write the records in Parquet format.
5. Read back and validate schema and record counts.
6. Clean up temporary test files.
