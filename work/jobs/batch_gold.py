import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from common.quakes import GOLD, PG_PROPS, PG_URL, SILVER

MAG_CLASSES = [(1, "micro", -10.0, 2.0), (2, "mineur", 2.0, 4.0), (3, "léger", 4.0, 5.0),
               (4, "modéré", 5.0, 6.0), (5, "fort", 6.0, 7.0), (6, "majeur", 7.0, 8.0), (7, "grand", 8.0, 99.0)]
DEPTH_CLASSES = [(1, "superficiel", -10.0, 70.0), (2, "intermédiaire", 70.0, 300.0), (3, "profond", 300.0, 1000.0)]


def class_key(col, classes):
    expr = None
    for key, _, lo, hi in classes:
        cond = (col >= lo) & (col < hi)
        expr = F.when(cond, key) if expr is None else expr.when(cond, key)
    return expr.cast("smallint")


def to_postgres(df: DataFrame, table: str):
    (df.write.mode("overwrite")
     .option("truncate", "true")
     .option("cascadeTruncate", "true")
     .option("batchsize", 10000)
     .jdbc(PG_URL, f"dw.{table}", properties=PG_PROPS))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", default=SILVER)
    ap.add_argument("--gold", default=GOLD)
    ap.add_argument("--no-postgres", action="store_true", help="n'écrit que le parquet gold (debug)")
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("quakes-gold")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.monotonic()

    quakes = (spark.read.parquet(a.silver)
              .where(F.col("event_type") == "earthquake")
              .withColumn("event_date", F.to_date("event_time")))
    quakes = quakes.cache()

    bounds = quakes.agg(F.min("event_date").alias("lo"), F.max("event_date").alias("hi")).first()
    dim_date = (spark.range(1)
                .select(F.explode(F.sequence(F.lit(bounds.lo), F.lit(bounds.hi))).alias("full_date"))
                .select(F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
                        "full_date",
                        F.year("full_date").cast("smallint").alias("year"),
                        F.quarter("full_date").cast("smallint").alias("quarter"),
                        F.month("full_date").cast("smallint").alias("month"),
                        F.dayofmonth("full_date").cast("smallint").alias("day"),
                        (((F.dayofweek("full_date") + 5) % 7) + 1).cast("smallint").alias("weekday")))

    dim_region = (quakes.select("region").distinct()
                  .withColumn("region_key", F.row_number().over(Window.orderBy("region")))
                  .select("region_key", "region"))

    dim_magnitude = spark.createDataFrame(MAG_CLASSES, "mag_class_key smallint, label string, min_mag double, max_mag double")
    dim_depth = spark.createDataFrame(DEPTH_CLASSES, "depth_class_key smallint, label string, min_km double, max_km double")

    fact = (quakes.join(F.broadcast(dim_region), "region")
            .select("event_id",
                    F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
                    "region_key",
                    class_key(F.col("mag"), MAG_CLASSES).alias("mag_class_key"),
                    class_key(F.col("depth_km"), DEPTH_CLASSES).alias("depth_class_key"),
                    "event_time", "mag", "depth_km", "lon", "lat", "sig", "felt", "tsunami", "alert"))
    fact = fact.cache()

    agg_region_month = (quakes.groupBy("region", F.year("event_time").cast("smallint").alias("year"),
                                       F.month("event_time").cast("smallint").alias("month"))
                        .agg(F.count("*").cast("int").alias("n_quakes"),
                             F.round(F.avg("mag"), 2).alias("avg_mag"),
                             F.max("mag").alias("max_mag")))

    agg_mag_distribution = (quakes.groupBy((F.floor(F.col("mag") * 10) / 10).alias("mag_bin"))
                            .agg(F.count("*").alias("n_quakes")))

    tables = [("dim_date", dim_date), ("dim_region", dim_region), ("dim_magnitude", dim_magnitude),
              ("dim_depth", dim_depth), ("fact_earthquake", fact),
              ("agg_region_month", agg_region_month), ("agg_mag_distribution", agg_mag_distribution)]

    for name, df in tables:
        df.write.mode("overwrite").parquet(f"{a.gold}/{name}")

    if not a.no_postgres:
        for name, df in tables:
            t = time.monotonic()
            to_postgres(df, name)
            print(f"  postgres dw.{name:<22} chargée en {time.monotonic() - t:5.1f} s")

    n_fact = fact.count()
    print("\n================ rapport gold ================")
    for name, df in tables:
        print(f"{name:<22} : {df.count():>10,} lignes")
    print(f"période                : {bounds.lo} -> {bounds.hi}")
    print(f"durée                  : {time.monotonic() - t0:.0f} s\n")
    print("répartition par classe de magnitude :")
    (fact.groupBy("mag_class_key").count().join(dim_magnitude, "mag_class_key")
     .select("mag_class_key", "label", "count", F.round(F.col("count") / n_fact * 100, 2).alias("pct"))
     .orderBy("mag_class_key").show(truncate=False))
    print("les 5 plus forts séismes :")
    (fact.join(dim_region, "region_key").orderBy(F.desc("mag"))
     .select("event_time", "mag", "region", "depth_km", "tsunami").show(5, truncate=False))

    fact.unpersist()
    quakes.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
