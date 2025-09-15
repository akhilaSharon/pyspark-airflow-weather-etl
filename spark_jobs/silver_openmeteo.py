# spark_jobs/silver_openmeteo.py
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="Partition date like 2025-09-15")
    p.add_argument("--bronze", default="s3a://bronze/openmeteo/")
    p.add_argument("--silver", default="s3a://silver/openmeteo/")
    return p.parse_args()

def main():
    args = parse_args()
    ds = args.date  # e.g., "2025-09-15"
    y, m, d = ds[:4], ds[5:7], ds[8:10]

    spark = (
        SparkSession.builder.appName(f"silver_openmeteo_{ds}")
        .getOrCreate()
    )

    # --- Read the raw JSON for that date ---
    src_path = f"{args.bronze}y={y}/m={m}/d={d}/"
    df = spark.read.json(src_path)  # Open-Meteo returns nested structs + arrays

    # The hourly section is an object containing arrays with equal length:
    # { time: [...], temperature_2m: [...], relative_humidity_2m: [...], precipitation: [...] }
    hourly = df.select("hourly", "latitude", "longitude", "timezone").limit(1)

    # Explode arrays into rows: zip arrays by index using arrays_zip
    hourly_cols = ["time", "temperature_2m", "relative_humidity_2m", "precipitation"]
    zipped = F.arrays_zip(*[F.col(f"hourly.{c}") for c in hourly_cols]).alias("z")

    rows = (
        hourly
        .select("latitude", "longitude", "timezone", zipped)
        .withColumn("row", F.explode("z"))
        .select(
            F.col("latitude").cast("double").alias("lat"),
            F.col("longitude").cast("double").alias("lon"),
            F.col("timezone").alias("tz"),
            F.col("row.time").alias("time"),
            F.col("row.temperature_2m").cast("double").alias("temperature_c"),
            F.col("row.relative_humidity_2m").cast("double").alias("humidity_pct"),
            F.col("row.precipitation").cast("double").alias("precip_mm"),
        )
        .withColumn("event_ts", F.to_timestamp("time"))  # to proper timestamp
        .drop("time")
    )

    # Add partitions
    out = (
        rows
        .withColumn("y", F.lit(y))
        .withColumn("m", F.lit(m))
        .withColumn("d", F.lit(d))
    )

    # Optional: control file count (avoid tiny files)
    out = out.coalesce(1)

    # --- Write to Silver as Parquet, partitioned by y/m/d ---
    dst_path = f"{args.silver}"
    (
        out.write
        .mode("overwrite")
        .partitionBy("y", "m", "d")
        .parquet(dst_path)
    )

    print(f"✅ Wrote Silver Parquet to {dst_path} (partitions y={y}/m={m}/d={d})")

if __name__ == "__main__":
    main()
