# projet big data : séismes USGS

architecture lambda : historique en **batch** + temps réel en **streaming**, réunis dans un data lake HDFS (bronze → silver → gold), servis par un data warehouse postgres, visualisés dans streamlit.

| couche | outils | fichiers |
|---|---|---|
| ingestion | script python (API FDSN), producer kafka (feed temps réel) | `work/ingestion/` |
| traitement | spark batch + spark structured streaming | `work/jobs/`, `work/common/` |
| persistance | HDFS (lake), postgres (warehouse) | `hadoop.env`, `sql/init.sql` |
| insights | streamlit (+ ML spark en bonus) | `dashboard/` |
| gouvernance | catalogue, colonnes de lineage, rôles postgres | `docs/catalogue.md`, `sql/init.sql` |

## arborescence

```
quakes-bigdata/
├── docker-compose.yml        9 services (kafka, kafka-init, namenode, datanode, spark-master, spark-worker, notebook, postgres, dashboard)
├── hadoop.env                config HDFS (réplication = 1)
├── docker/Dockerfile         image spark + connecteurs kafka/postgres + libs python
├── sql/init.sql              schéma du warehouse : live_quakes + étoile gold + rôles
├── dashboard/                app streamlit (lecture seule)
├── docs/catalogue.md         dictionnaire des données (gouvernance)
└── work/                     monté dans /home/jovyan/work sur notebook, master et worker
    ├── common/quakes.py      schéma + nettoyage partagés batch/streaming
    ├── ingestion/batch_history.py
    ├── ingestion/producer_realtime.py
    ├── jobs/streaming_realtime.py
    ├── jobs/batch_silver.py
    ├── jobs/batch_gold.py
    ├── jobs/ml_zones.py
    ├── checkpoints/  logs/
```

## 0. démarrer la stack

```bash
docker compose up -d --build        # 1re fois : ~5-10 min (images + téléchargement des jars)
docker compose ps                   # tout en "Up", sauf kafka-init en "Exited (0)" : c'est normal, il a créé le topic et s'est arrêté
docker compose logs kafka-init      # doit afficher "Topic: quakes_raw  PartitionCount: 3"
```

| url | attendu |
|---|---|
| http://localhost:8888 | jupyterlab, sans mot de passe |
| http://localhost:8080 | master spark, **Alive Workers: 1** |
| http://localhost:9870 | HDFS, onglet Datanodes : **1 live node** |
| http://localhost:8501 | dashboard (vide au début, normal) |

⚠️ les commandes python/spark ci-dessous se lancent dans un **terminal jupyter** (File → New → Terminal), pas sur windows : c'est là que `kafka`, `namenode`, `postgres` et `spark-master` existent par leur nom.

## 1. streaming (florent)

```bash
cd ~/work
# producer : feed USGS -> kafka
nohup python -u ingestion/producer_realtime.py > logs/producer.log 2>&1 &
tail -f logs/producer.log           # Ctrl+C quitte le tail, PAS le producer
```
attendu toutes les 60 s : `feed: 12 séismes | envoyés: 12 ...` au 1er tour, puis surtout des `envoyés: 0` à `3`.

```bash
# job streaming : kafka -> bronze HDFS + postgres. LANCER AVANT le batch (il ne prend qu'1 cœur sur 2)
nohup spark-submit --master spark://spark-master:7077 \
  --conf spark.cores.max=1 --conf spark.executor.memory=1g --conf spark.ui.port=4041 \
  jobs/streaming_realtime.py > logs/streaming.log 2>&1 &
grep "upsertés" logs/streaming.log
```
attendu : `[batch N] X séismes upsertés dans dw.live_quakes`, et la carte de http://localhost:8501 se remplit.

vérifier dans kafka (powershell) :
```powershell
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic quakes_raw --from-beginning --max-messages 3 --property print.key=true
```

## 2. historique (fred)

```bash
cd ~/work
# test sur 1 mois d'abord (~1 min)
python ingestion/batch_history.py --start 2024-01 --end 2024-02
# puis le vrai run, en arrière-plan (long : plusieurs dizaines de minutes, reprend où il s'est arrêté si on le relance)
nohup python -u ingestion/batch_history.py --start 2010-01 > logs/history.log 2>&1 &
tail -f logs/history.log
```
vérifier : http://localhost:9870 → Utilities → Browse the file system → `/datalake/bronze/usgs_history`

## 3. silver

```bash
cd ~/work
spark-submit --master spark://spark-master:7077 \
  --conf spark.cores.max=1 --conf spark.executor.memory=2g --driver-memory 2g \
  jobs/batch_silver.py
```
à la fin, un rapport qualité : lignes lues, rejetées, doublons supprimés, top régions, période couverte. **copiez-le dans le rapport final**, c'est de la matière pour la partie "étapes suivies".

## 4. gold (étoile + chargement postgres)

```bash
cd ~/work
spark-submit --master spark://spark-master:7077 \
  --conf spark.cores.max=1 --conf spark.executor.memory=2g --driver-memory 2g \
  jobs/batch_gold.py
```
attendu : 7 lignes `postgres dw.xxx chargée en N s`, puis le rapport (lignes par table, répartition par classe de magnitude, top 5 des plus forts).
relançable autant de fois que voulu : chaque table est vidée puis rechargée.

## 5. ML bonus : zones sismiques

```bash
spark-submit --master spark://spark-master:7077 --conf spark.cores.max=1 jobs/ml_zones.py
```
teste k = 8, 12, 16, 20, 25, affiche le score silhouette de chacun, garde le meilleur et charge `dw.ml_zones`.

## 6. dashboard

http://localhost:8501, 3 onglets : temps réel, analyses historiques (filtres période + magnitude), zones sismiques.
`dashboard/` est monté dans le conteneur : une modif de `app.py` est rechargée à la sauvegarde.

## ordre complet du pipeline

```
producer_realtime.py  ──► kafka ──► streaming_realtime.py ──► bronze/usgs_realtime + dw.live_quakes
batch_history.py      ──────────────────────────────────────► bronze/usgs_history
                                         batch_silver.py  ──► silver/earthquakes
                                         batch_gold.py    ──► gold/* + dw.dim_*, dw.fact_earthquake, dw.agg_*
                                         ml_zones.py      ──► gold/ml_zones + dw.ml_zones
```
pour intégrer les nouveaux séismes temps réel dans les analyses historiques : relancer silver puis gold.

## arrêter / remettre à zéro

```bash
pkill -f producer_realtime.py           # dans le terminal jupyter
pkill -f streaming_realtime.py
docker compose down                     # arrête tout, garde les données
docker compose down -v                  # ⚠️ efface HDFS, kafka et postgres (init.sql sera rejoué)
```
pour retraiter le streaming depuis le début : arrêter le job, puis `hdfs dfs -rm -r /datalake/_checkpoints` depuis `docker exec -it namenode bash`.

## dépannage

| symptôme | cause probable |
|---|---|
| `NoBrokersAvailable` | script lancé sur windows au lieu du terminal jupyter, ou kafka pas encore prêt |
| le job spark reste en `WAITING` | plus de cœurs libres : un autre job prend tout. vérifie http://localhost:8080 |
| `Connection refused namenode:9000` | le namenode démarre encore, attends 30 s |
| la carte reste vide | `grep -i error logs/streaming.log`, et vérifier que le producer envoie bien |
| port 8888 déjà pris | un jupyter local tourne déjà sur windows : change `"8888:8888"` en `"8889:8888"` |
