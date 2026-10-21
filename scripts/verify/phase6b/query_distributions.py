import sys
from pyspark.sql import functions as F
from src.utils.spark_session import get_spark_session

def main():
    spark = get_spark_session("query_distributions")
    df = spark.read.parquet("hdfs://localhost:9000/user/saniya/flight_delay/processed/flights_enriched")
    
    print("--- DISTANCE_BAND Distribution ---")
    df.groupBy("DISTANCE_BAND").count().orderBy(F.desc("count")).show()

    print("--- DELAY_CATEGORY Distribution ---")
    df.groupBy("DELAY_CATEGORY").count().orderBy(F.desc("count")).show()

    print("--- PRIMARY_DELAY_CAUSE Distribution ---")
    df.groupBy("PRIMARY_DELAY_CAUSE").count().orderBy(F.desc("count")).show()

    print("--- Null Coordinates Summary ---")
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c + "_nulls")
        for c in ["ORIGIN_LAT", "DEST_LAT", "ORIGIN_LON", "DEST_LON"]
    ]).show()

    print("--- Null ORIGIN_LAT by Airport ---")
    df.filter(F.col("ORIGIN_LAT").isNull()).groupBy("ORIGIN_AIRPORT").count().orderBy(F.desc("count")).show()

    print("--- Null DEST_LAT by Airport ---")
    df.filter(F.col("DEST_LAT").isNull()).groupBy("DESTINATION_AIRPORT").count().orderBy(F.desc("count")).show()

    spark.stop()

if __name__ == "__main__":
    main()
