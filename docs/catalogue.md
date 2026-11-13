# catalogue des données (gouvernance)

## zones du data lake (HDFS)

| zone | chemin | format | contenu | écrit par |
|---|---|---|---|---|
| bronze historique | `/datalake/bronze/usgs_history/year=/month=/quakes.geojson` | GeoJSON (FeatureCollection) | réponse brute de l'API FDSN + bloc `_lineage` | `ingestion/batch_history.py` |
| bronze temps réel | `/datalake/bronze/usgs_realtime/ingest_date=/part-*.txt` | JSON lines (1 feature par ligne) | messages kafka bruts | `jobs/streaming_realtime.py` |
| silver | `/datalake/silver/earthquakes/year=/month=/` | parquet | 1 ligne par séisme, nettoyé, dédoublonné | `jobs/batch_silver.py` |
| gold | `/datalake/gold/<table>/` | parquet | étoile + agrégats, copie identique de ce qui est chargé dans postgres | `jobs/batch_gold.py`, `jobs/ml_zones.py` |
| checkpoints | `/datalake/_checkpoints/` | interne spark | offsets kafka déjà traités | streaming |

## silver : `earthquakes`

| colonne | type | description | origine USGS |
|---|---|---|---|
| event_id | string | identifiant unique USGS (clé de dédoublonnage) | `id` |
| event_time | timestamp UTC | heure du séisme | `properties.time` (epoch ms) |
| updated_at | timestamp UTC | dernière révision USGS : la plus récente gagne | `properties.updated` |
| mag | double | magnitude | `properties.mag` |
| mag_type | string | échelle de magnitude (ml, mb, mww...) | `properties.magType` |
| place | string | description textuelle du lieu | `properties.place` |
| region | string | **dérivée** : texte après la dernière virgule de `place`, états US développés (CA → California) | calcul |
| lon, lat | double | épicentre | `geometry.coordinates[0..1]` |
| depth_km | double | profondeur du foyer | `geometry.coordinates[2]` |
| event_type | string | earthquake, quarry blast, explosion... | `properties.type` |
| status | string | automatic (non revu par un humain) / reviewed | `properties.status` |
| tsunami | boolean | alerte tsunami émise | `properties.tsunami == 1` |
| sig | int | indice d'importance USGS (0-1000+) | `properties.sig` |
| felt | int | nombre de signalements "ressenti" | `properties.felt` |
| alert | string | niveau PAGER : green, yellow, orange, red | `properties.alert` |
| net, nst, gap, rms | divers | qualité de la mesure (réseau, nb stations, couverture, erreur) | `properties.*` |
| url | string | fiche USGS de l'événement | `properties.url` |
| _source | string | **lineage** : usgs_fdsn ou usgs_feed_all_hour | ajouté |
| _ingested_at | timestamp | **lineage** : date de calcul de la silver | ajouté |
| _batch_id | string | **lineage** : identifiant du run spark | ajouté |
| year, month | int | colonnes de partition (dérivées de event_time) | calcul |

## gold : schéma en étoile (postgres `dw` + HDFS `/datalake/gold`)

```
                 dim_date (date_key AAAAMMJJ, year, quarter, month, day, weekday)
                     │
dim_region ──── fact_earthquake ──── dim_magnitude (micro <2, mineur 2-4, léger 4-5,
(region_key,     (event_id, mag,       modéré 5-6, fort 6-7, majeur 7-8, grand ≥8)
 region)          depth_km, lat, lon,
                  sig, felt, tsunami)
                     │
                 dim_depth (superficiel <70 km, intermédiaire 70-300, profond >300)
```

| table | grain (1 ligne =) | remarque |
|---|---|---|
| fact_earthquake | 1 séisme (event_type = earthquake uniquement) | clés vers les 4 dimensions + mesures |
| dim_date | 1 jour du calendrier, y compris les jours sans séisme | |
| dim_region | 1 région (dérivée de `place`) | clé attribuée par ordre alphabétique à chaque run |
| agg_region_month | 1 région × 1 mois | nb, magnitude moyenne et max |
| agg_mag_distribution | 1 tranche de 0.1 de magnitude | sert à la loi de gutenberg-richter |
| ml_zones | 1 zone sismique trouvée par KMeans | centre lat/lon, nb séismes, k et silhouette |

## règles qualité (silver)

une ligne est rejetée si : `event_id` ou `event_time` null, `mag` null ou hors [-2, 10], lat hors [-90, 90], lon hors [-180, 180], profondeur hors [-10, 800] km.
dédoublonnage : une seule ligne par `event_id`, celle avec le `updated_at` le plus récent.

## accès (postgres)

| rôle | droits | utilisé par |
|---|---|---|
| admin | superuser | administration uniquement |
| etl_writer | propriétaire du schéma `dw` (lecture + écriture) | jobs spark |
| dashboard_reader | SELECT uniquement sur `dw` | streamlit |
