import sys
from pathlib import Path
sys.path.insert(0, "/home/saniya/flight-delay-analytics")
from src.utils.spark_session import get_spark_session

spark = get_spark_session("check_cols")
df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_enriched")
cols = df.columns
print(f"Total columns in flights_enriched: {len(cols)}")
for i, c in enumerate(cols, 1):
    print(f"{i:2d}. {c}")
spark.stop()
