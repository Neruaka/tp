"""
ingestion streaming : récupération en streaming des données quake -> kafka (topic quakes_raw)

- l'argument' "all_hour" contient tous les séismes de la dernière heure, les données se récupèrent toutes les minutes
- on l'interroge toutes les 60 s -> d'un appel à l'autre, ~59 minutes de séismes sont DÉJÀ connues
- il est possible que l'USGS MAJ un séisme (ex: magnitude, coordonnées, etc.) -> le champ `updated` change

On envoie un séisme si nouveau ou a mettre jour si l'ajout du séisme date de moins de 3h
c'est une première couche de dédoublonnage ;
(spark garde la version la plus récente par id)

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
INTERVAL_S = 60 # Intervalle de récupération des séismes (en secondes)
FORGET_AFTER_MS = 3 * 3600 * 1000   # On oublie les séismes vus il y a + de 3 h (mémoire bornée)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")

"""
Mis à jour ou ajout de séisme à mettre dans le flux Kafka
"""
def new_or_updated(features, seen: dict) -> list:
    """Garde les features jamais vues ou dont la version (updated) a changé. met à jour `seen`."""
    out = []
    for f in features:
        fid, upd = f["id"], f["properties"].get("updated")
        if seen.get(fid) != upd:
            seen[fid] = upd
            out.append(f)
    return out

"""
Mécanisme d'oublie de séismes vus il y a longtemps (mémoire bornée)
"""
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
        # on temporise pour que le prochain appel se fasse toutes les INTERVAL_S secondes
        while running and time.monotonic() - t0 < INTERVAL_S:
            time.sleep(1)

    producer.flush()
    producer.close()
    log.info("producer arrêté proprement")


if __name__ == "__main__":
    main()
