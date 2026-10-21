import sys
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

def main():
    spark = get_spark_session("task3_queries")
    df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_clean")

    print("--- flights_clean: AA 803 on 2015-08-29 ---")
    df_stt = df.filter(
        (F.col("MONTH") == 8) & 
        (F.col("DAY") == 29) & 
        (F.col("AIRLINE") == "AA") & 
        (F.col("FLIGHT_NUMBER") == 803)
    )
    df_stt.show(vertical=True, truncate=False)

    print("--- flights_clean: SCHEDULED_TIME IS NULL ---")
    df_null = df.filter(F.col("SCHEDULED_TIME").isNull())
    df_null.show(vertical=True, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
