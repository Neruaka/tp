"""
job spark batch : bronze (historique + temps réel) -> silver

entrées :
  - bronze/usgs_history  : FeatureCollections GeoJSON (1 fichier = 1 mois), multi-ligne
  - bronze/usgs_realtime : 1 feature JSON par ligne (sortie du streaming)
sortie :
  - silver/earthquakes : parquet, 1 ligne par séisme, partitionné par year/month

étapes : lecture avec schéma imposé -> aplatissement -> règles qualité
         -> dédoublonnage (1 seule version par event_id : la plus récente) -> écriture

idempotent : la silver est entièrement recalculée depuis le bronze à chaque run (mode overwrite).
le bronze ne bouge jamais, donc relancer donne toujours le même résultat.

usage :
    cd ~/work
    spark-submit --master spark://spark-master:7077 jobs/batch_silver.py
    (options : --bronze-history / --bronze-realtime / --silver pour tester en local)
"""
import argparse
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import ArrayType, StructField, StructType

from common.quakes import (BRONZE_HISTORY, BRONZE_REALTIME, FEATURE_SCHEMA, SILVER, flatten,
                           quality_filter)


def path_exists(spark, path: str) -> bool:
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def read_history(spark, path):
    """un fichier = {"type":..., "_lineage":..., "features":[...]} -> une ligne par feature"""
    collection_schema = StructType([StructField("features", ArrayType(FEATURE_SCHEMA))])
    return (spark.read.schema(collection_schema).option("multiLine", True)
            .option("recursiveFileLookup", True)
            .json(path)
            .select(F.explode("features").alias("f")).select("f.*")
            .withColumn("_source", F.lit("usgs_fdsn")))


def read_realtime(spark, path):
    return (spark.read.schema(FEATURE_SCHEMA).json(path)
            .drop("ingest_date")
            .withColumn("_source", F.lit("usgs_feed_all_hour")))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bronze-history", default=BRONZE_HISTORY)
    ap.add_argument("--bronze-realtime", default=BRONZE_REALTIME)
    ap.add_argument("--silver", default=SILVER)
    a = ap.parse_args()

    spark = (SparkSession.builder.appName("quakes-silver")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "16")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.monotonic()
    batch_id = uuid.uuid4().hex[:12]

    sources = []
    for path, reader in [(a.bronze_history, read_history), (a.bronze_realtime, read_realtime)]:
        if path_exists(spark, path):
            sources.append(reader(spark, path))
        else:
            print(f"(source absente, ignorée : {path})")
    if not sources:
        sys.exit("aucune donnée bronze trouvée : lance d'abord l'ingestion")

    raw = sources[0]
    for s in sources[1:]:
        raw = raw.unionByName(s)

    flat = flatten(raw, keep=["_source"])

    flat = flat.cache()
    n_raw = flat.count()

    clean = quality_filter(flat)

    w = Window.partitionBy("event_id").orderBy(F.col("updated_at").desc_nulls_last())
    silver = (clean.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
              .withColumn("year", F.year("event_time"))
              .withColumn("month", F.month("event_time"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_batch_id", F.lit(batch_id)))

    (silver.repartition("year", "month")
     .write.mode("overwrite").partitionBy("year", "month").parquet(a.silver))

    out = spark.read.parquet(a.silver)
    n_clean = clean.count()
    n_silver = out.count()
    print("\n================ rapport silver ================")
    print(f"batch_id                     : {batch_id}")
    print(f"lignes lues dans le bronze    : {n_raw:>10,}")
    print(f"rejetées par la qualité       : {n_raw - n_clean:>10,}  ({(n_raw - n_clean) / max(n_raw, 1):.2%})")
    print(f"doublons supprimés            : {n_clean - n_silver:>10,}")
    print(f"séismes uniques en silver     : {n_silver:>10,}")
    print(f"durée                         : {time.monotonic() - t0:.0f} s")
    print("\npar source :")
    out.groupBy("_source").count().orderBy("_source").show(truncate=False)
    print("par type d'événement (on ne garde que 'earthquake' en gold) :")
    out.groupBy("event_type").count().orderBy(F.desc("count")).show(10, truncate=False)
    print("top 10 régions :")
    out.where("event_type = 'earthquake'").groupBy("region").count().orderBy(F.desc("count")).show(10, truncate=False)
    print("période couverte :")
    out.select(F.min("event_time").alias("premier"), F.max("event_time").alias("dernier")).show(truncate=False)

    flat.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
