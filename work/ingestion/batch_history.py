"""
    python ingestion/batch_history.py --start 2024-01 --end 2024-02     # test : 1 mois
    python ingestion/batch_history.py --start 2010-01                   # le vrai run
"""
import argparse
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests
from hdfs import InsecureClient

FDSN = "https://earthquake.usgs.gov/fdsnws/event/1"
MAX_EVENTS = 20000
BRONZE_DIR = "/datalake/bronze/usgs_history"
PAUSE_S = 1.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("batch_history")


def month_range(start: date, end: date):
    cur = start.replace(day=1)
    while cur < end:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield cur, min(nxt, end)
        cur = nxt


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def count_events(session, t0: datetime, t1: datetime) -> int:
    r = session.get(f"{FDSN}/count", params={"format": "geojson", "starttime": iso(t0), "endtime": iso(t1)}, timeout=60)
    r.raise_for_status()
    return int(r.json()["count"])


def fetch_features(session, t0: datetime, t1: datetime) -> list:
    """récupère les features entre t0 et t1, en coupant l'intervalle si > 20000 événements"""
    n = count_events(session, t0, t1)
    if n > MAX_EVENTS:
        mid = t0 + (t1 - t0) / 2
        log.info("  %s -> %s : %d événements > %d, on coupe en deux", iso(t0), iso(t1), n, MAX_EVENTS)
        return fetch_features(session, t0, mid) + fetch_features(session, mid, t1)
    time.sleep(PAUSE_S)
    for attempt in range(1, 4):
        try:
            r = session.get(f"{FDSN}/query", params={
                "format": "geojson", "starttime": iso(t0), "endtime": iso(t1),
                "orderby": "time-asc", "limit": MAX_EVENTS,
            }, timeout=300)
            r.raise_for_status()
            return r.json()["features"]
        except (requests.RequestException, ValueError) as e:
            log.warning("  essai %d/3 échoué (%s), nouvelle tentative dans %ds", attempt, e, 5 * attempt)
            time.sleep(5 * attempt)
    raise RuntimeError(f"impossible de récupérer {iso(t0)} -> {iso(t1)}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2010-01", help="premier mois, format AAAA-MM")
    p.add_argument("--end", default=None, help="mois de fin EXCLU, format AAAA-MM (défaut : mois courant inclus)")
    p.add_argument("--hdfs", default="http://namenode:9870")
    p.add_argument("--force", action="store_true", help="retélécharge les mois déjà présents")
    a = p.parse_args()

    start = datetime.strptime(a.start, "%Y-%m").date()
    if a.end:
        end = datetime.strptime(a.end, "%Y-%m").date()
    else:
        today = date.today()
        end = (today.replace(day=28) + timedelta(days=4)).replace(day=1)

    client = InsecureClient(a.hdfs, user="jovyan")
    session = requests.Session()
    session.headers["User-Agent"] = "ipssi-bigdata-student-project"

    total, t_start = 0, time.monotonic()
    for m0, m1 in month_range(start, end):
        path = f"{BRONZE_DIR}/year={m0.year}/month={m0.month:02d}/quakes.geojson"
        if not a.force and client.status(path, strict=False) is not None:
            log.info("%s déjà présent, on saute", path)
            continue

        t0 = datetime(m0.year, m0.month, m0.day, tzinfo=timezone.utc)
        t1 = datetime(m1.year, m1.month, m1.day, tzinfo=timezone.utc)
        features = fetch_features(session, t0, t1)

        payload = {
            "type": "FeatureCollection",
            "_lineage": {
                "source": "usgs_fdsn",
                "starttime": iso(t0), "endtime": iso(t1),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "n_features": len(features),
            },
            "features": features,
        }
        client.write(path, data=json.dumps(payload), overwrite=True, encoding="utf-8", replication=1)
        total += len(features)
        log.info("%s : %6d séismes écrits (total %d)", path, len(features), total)

    log.info("terminé : %d séismes en %.0f s", total, time.monotonic() - t_start)


if __name__ == "__main__":
    main()
