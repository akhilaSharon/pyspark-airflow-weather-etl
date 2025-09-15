import argparse
from pyspark.sql import SparkSession, functions as F

def args_():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True)
    p.add_argument("--gold", default="s3a://gold/openmeteo/")
    p.add_argument("--pg_url", default="jdbc:postgresql://postgres:5432/airflow")
    p.add_argument("--pg_user", default="airflow")
    p.add_argument("--pg_pass", default="airflow")
    return p.parse_args()

def main():
    a = args_()
    y, m, d = a.date[:4], a.date[5:7], a.date[8:10]

    spark = SparkSession.builder.appName(f"load_gold_pg_{a.date}").getOrCreate()

    # Read the day’s partition; basePath helps (but we'll also force y/m/d below)
    df = (spark.read
          .option("basePath", a.gold)
          .parquet(f"{a.gold}y={y}/m={m}/d={d}/"))

    # Ensure date columns exist and types are stable
    df = (df
          .withColumn("y", F.lit(int(y)))
          .withColumn("m", F.lit(int(m)))
          .withColumn("d", F.lit(int(d)))
          .select(
              "y","m","d",
              "min_temp_c","max_temp_c","avg_temp_c",
              "precip_mm_sum","avg_humidity_pct"
          ))

    # Write to STAGE (append); upsert happens in the next step
    (df.write.format("jdbc")
       .option("url", a.pg_url)
       .option("dbtable", "weather_daily_stage")
       .option("user", a.pg_user)
       .option("password", a.pg_pass)
       .option("driver", "org.postgresql.Driver")
       .mode("append")
       .save())

    print(f"✅ Staged Gold for {a.date} into Postgres table weather_daily_stage")

if __name__ == "__main__":
    main()
