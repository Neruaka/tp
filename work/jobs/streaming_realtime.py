"""
job spark structured streaming : kafka (quakes_raw) -> 2 sorties en parallèle

  1. bronze temps réel sur HDFS : le message brut, tel quel, en JSON lines
     /datalake/bronze/usgs_realtime/ingest_date=AAAA-MM-JJ/part-*.json
  2. table dw.live_quakes dans postgres : version propre, upsert sur event_id
     -> c'est elle que la carte "dernières 24h" du dashboard lit

chaque sortie a SON checkpoint (le marque-page de spark, cf. TP kafka) sur HDFS.

usage (terminal jupyter, laisser tourner) :
    cd ~/work
    nohup spark-submit --master spark://spark-master:7077 \
        --conf spark.cores.max=1 --conf spark.ui.port=4041 \
        jobs/streaming_realtime.py > logs/streaming.log 2>&1 &
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F

from common.quakes import BRONZE_REALTIME, CHECKPOINTS, FEATURE_SCHEMA, flatten, quality_filter

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PG = dict(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")), dbname=os.getenv("PG_DB", "quakes"),
          user=os.getenv("PG_USER", "etl_writer"), password=os.getenv("PG_PASSWORD", "etl_writer"))

LIVE_COLS = ["event_id", "event_time", "updated_at", "mag", "mag_type", "place", "region", "lon", "lat",
             "depth_km", "event_type", "status", "tsunami", "sig", "alert", "url"]

UPSERT_SQL = f"""
INSERT INTO dw.live_quakes ({", ".join(LIVE_COLS)}, _batch_id)
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    {", ".join(f"{c} = EXCLUDED.{c}" for c in LIVE_COLS[1:])},
    _batch_id = EXCLUDED._batch_id,
    _ingested_at = now()
WHERE EXCLUDED.updated_at > dw.live_quakes.updated_at
"""


def upsert_live(batch_df, batch_id: int):
    """appelée par spark à chaque micro-batch (foreachBatch). un micro-batch = quelques dizaines
    de lignes max, donc on peut le ramener sur le driver et l'écrire en une transaction."""
    latest = (batch_df.groupBy("event_id")
              .agg(F.max_by(F.struct(*LIVE_COLS), "updated_at").alias("r"))
              .select("r.*"))
    rows = [tuple(r[c] for c in LIVE_COLS) + (batch_id,) for r in latest.collect()]
    if not rows:
        return
    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows)
    finally:
        conn.close()
    print(f"[batch {batch_id}] {len(rows)} séismes upsertés dans dw.live_quakes", flush=True)


def main():
    spark = (SparkSession.builder.appName("quakes-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", "quakes_raw")
           .option("startingOffsets", "earliest")
           .load()
           .select(F.col("key").cast("string").alias("key"),
                   F.col("value").cast("string").alias("value"),
                   F.col("timestamp").alias("kafka_ts")))

    bronze_q = (raw.select("value", F.to_date("kafka_ts").cast("string").alias("ingest_date"))
                .writeStream.format("text")
                .partitionBy("ingest_date")
                .option("path", BRONZE_REALTIME)
                .option("checkpointLocation", f"{CHECKPOINTS}/bronze_realtime")
                .trigger(processingTime="1 minute")
                .queryName("bronze_realtime")
                .start())

    clean = quality_filter(flatten(raw.select(F.from_json("value", FEATURE_SCHEMA).alias("f")).select("f.*")))
    live_q = (clean.writeStream
              .foreachBatch(upsert_live)
              .option("checkpointLocation", f"{CHECKPOINTS}/live_quakes")
              .trigger(processingTime="30 seconds")
              .queryName("live_quakes")
              .start())

    print("streaming démarré : bronze_realtime + live_quakes", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
