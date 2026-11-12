"""
Document reference sur la connexion des données et les paths
La logique est partagé entre la logique géré par Spark et celle testee par le producteur kafka. Il y a
une seule définition du schéma et du nettoyage -> batch et streaming produisent EXACTEMENT les mêmes colonnes.
"""
import os

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

HDFS = "hdfs://namenode:9000"
BRONZE_HISTORY = f"{HDFS}/datalake/bronze/usgs_history"
BRONZE_REALTIME = f"{HDFS}/datalake/bronze/usgs_realtime"
SILVER = f"{HDFS}/datalake/silver/earthquakes"
GOLD = f"{HDFS}/datalake/gold"
CHECKPOINTS = f"{HDFS}/datalake/_checkpoints"

#Data warehouse: postgres
PG_URL = (f"jdbc:postgresql://{os.getenv('PG_HOST', 'postgres')}:{os.getenv('PG_PORT', '5432')}"
          f"/{os.getenv('PG_DB', 'quakes')}")
PG_PROPS = {"user": os.getenv("PG_USER", "etl_writer"), "password": os.getenv("PG_PASSWORD", "etl_writer"),
            "driver": "org.postgresql.Driver"}

# Schéma GEOJSONS des séismes rapportés par l'API USGS
# On l'impose à la lecture ("schema on read" du cours) : spark n'a pas à le deviner,
# et un champ inattendu ne casse rien ou typage non-correct.
FEATURE_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("properties", StructType([
        StructField("mag", DoubleType()),
        StructField("magType", StringType()),
        StructField("place", StringType()),
        StructField("time", LongType()),        # epoch Linux en millisecondes
        StructField("updated", LongType()),
        StructField("felt", IntegerType()),
        StructField("alert", StringType()),
        StructField("status", StringType()),
        StructField("tsunami", IntegerType()),
        StructField("sig", IntegerType()),
        StructField("net", StringType()),
        StructField("nst", IntegerType()),
        StructField("gap", DoubleType()),
        StructField("rms", DoubleType()),
        StructField("type", StringType()),
        StructField("url", StringType()),
    ])),
    StructField("geometry", StructType([
        StructField("coordinates", ArrayType(DoubleType())),   # [lon, lat, profondeur_km]
    ])),
    StructField("_lineage", StructType([
        StructField("source", StringType()),
        StructField("fetched_at", StringType()),
    ])),
])

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts",
    "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri", "MT": "Montana",
    "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "PR": "Puerto Rico", "MX": "Mexico", "B.C.": "Baja California",
}


def region_from_place(place: Column) -> Column:
    """'10 km SW of Tokyo, Japan' -> 'Japan' ; 'Mid-Atlantic Ridge' -> 'Mid-Atlantic Ridge' ;
    '5 km NW of The Geysers, CA' -> 'California'. vide/null -> 'Unknown'"""
    last = F.trim(F.element_at(F.split(place, ","), -1))
    mapping = F.create_map(*[F.lit(x) for kv in US_STATES.items() for x in kv])
    region = F.coalesce(mapping[last], last)
    return F.when(place.isNull() | (F.trim(place) == ""), F.lit("Unknown")).otherwise(region)


def flatten(features: DataFrame, keep=()) -> DataFrame:
    """features GeoJSON (colonnes id/properties/geometry) -> une ligne propre par séisme.
    `keep` : colonnes supplémentaires à garder telles quelles (ex : _source)"""
    p, c = F.col("properties"), F.col("geometry.coordinates")
    return features.select(
        *[F.col(k) for k in keep],
        F.col("id").alias("event_id"),
        F.timestamp_millis(p.time).alias("event_time"),
        F.timestamp_millis(p.updated).alias("updated_at"),
        p.mag.alias("mag"),
        p.magType.alias("mag_type"),
        p.place.alias("place"),
        region_from_place(p.place).alias("region"),
        c[0].alias("lon"),
        c[1].alias("lat"),
        c[2].alias("depth_km"),
        p.type.alias("event_type"),
        p.status.alias("status"),
        (p.tsunami == 1).alias("tsunami"),
        p.sig.alias("sig"),
        p.felt.alias("felt"),
        p.alert.alias("alert"),
        p.net.alias("net"),
        p.nst.alias("nst"),
        p.gap.alias("gap"),
        p.rms.alias("rms"),
        p.url.alias("url"),
    )


def quality_filter(df: DataFrame) -> DataFrame:
    """règles de qualité : on jette ce qui est inexploitable ou physiquement absurde"""
    return df.where(
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("lat").between(-90, 90)
        & F.col("lon").between(-180, 180)
        & F.col("mag").isNotNull() & F.col("mag").between(-2, 10)
        & F.col("depth_km").between(-10, 800)    # négatif possible : au-dessus du niveau de la mer
    )
