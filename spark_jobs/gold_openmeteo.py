# spark_jobs/gold_openmeteo.py
import argparse
from pyspark.sql import SparkSession, functions as F

def args_():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)             # e.g. 2025-09-15
    p.add_argument("--silver", default="s3a://silver/openmeteo/")
    p.add_argument("--gold",   default="s3a://gold/openmeteo/")
    return p.parse_args()

def main():
    a = args_()
    y, m, d = a.date[:4], a.date[5:7], a.date[8:10]

    spark = SparkSession.builder.appName(f"gold_openmeteo_{a.date}").getOrCreate()

    df = spark.read.parquet(f"{a.silver}y={y}/m={m}/d={d}/")

    daily = (df
      .agg(
        F.min("temperature_c").alias("min_temp_c"),
        F.max("temperature_c").alias("max_temp_c"),
        F.avg("temperature_c").alias("avg_temp_c"),
        F.sum("precip_mm").alias("precip_mm_sum"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
      )
      .withColumn("y", F.lit(y)).withColumn("m", F.lit(m)).withColumn("d", F.lit(d))
    )

    (daily.coalesce(1)
      .write.mode("overwrite")
      .partitionBy("y","m","d")
      .parquet(a.gold))

    print(f"✅ Gold written to s3a://gold/openmeteo/ y={y}/m={m}/d={d}")

if __name__ == "__main__":
    main()
