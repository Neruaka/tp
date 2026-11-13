"""
ingestion streaming : feed temps réel USGS -> kafka (topic quakes_raw)

- le feed "all_hour" contient tous les séismes de la dernière heure, mis à jour ~chaque minute
- on l'interroge toutes les 60 s -> d'un appel à l'autre, ~59 minutes de séismes sont DÉJÀ connues
- et l'USGS révise souvent un séisme après coup (magnitude corrigée, statut "reviewed"...)
  -> le champ `updated` change

donc on n'envoie un séisme que s'il est nouveau OU si son `updated` a changé.
c'est une première couche de dédoublonnage ; la vraie garantie est en aval
(spark garde la version la plus récente par id, postgres fait un upsert).

clé du message kafka = id du séisme -> toutes les versions d'un même séisme
tombent dans la même partition, donc dans l'ordre.

usage (terminal jupyter) :
    cd ~/work
    nohup python -u ingestion/producer_realtime.py > logs/producer.log 2>&1 &
    tail -f logs/producer.log
"""
import json
import logging
import signal
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
TOPIC = "quakes_raw"
BOOTSTRAP = "kafka:9092"
INTERVAL_S = 60
FORGET_AFTER_MS = 3 * 3600 * 1000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def new_or_updated(features, seen: dict) -> list:
    """garde les features jamais vues ou dont la version (updated) a changé. met à jour `seen`."""
    out = []
    for f in features:
        fid, upd = f["id"], f["properties"].get("updated")
        if seen.get(fid) != upd:
            seen[fid] = upd
            out.append(f)
    return out


def forget_old(seen: dict, now_ms: int):
    for fid in [k for k, upd in seen.items() if upd is None or now_ms - upd > FORGET_AFTER_MS]:
        del seen[fid]


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )
    session = requests.Session()
    session.headers["User-Agent"] = "ipssi-bigdata-student-project"
    seen: dict = {}
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("démarré -> topic=%s, toutes les %ds", TOPIC, INTERVAL_S)
    while running:
        t0 = time.monotonic()
        try:
            r = session.get(FEED_URL, timeout=30)
            r.raise_for_status()
            feed = r.json()
            fetched_at = datetime.now(timezone.utc).isoformat()
            to_send = new_or_updated(feed["features"], seen)
            for f in to_send:
                f["_lineage"] = {"source": "usgs_feed_all_hour", "fetched_at": fetched_at}
                producer.send(TOPIC, key=f["id"], value=f)
            producer.flush()
            forget_old(seen, feed["metadata"]["generated"])
            log.info("feed: %3d séismes | envoyés: %3d (nouveaux ou révisés) | en mémoire: %d",
                     len(feed["features"]), len(to_send), len(seen))
        except (requests.RequestException, ValueError, KeyError) as e:
            log.warning("échec de l'appel au feed : %s (on réessaie au prochain tour)", e)
        while running and time.monotonic() - t0 < INTERVAL_S:
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info("producer arrêté proprement")


if __name__ == "__main__":
    main()
