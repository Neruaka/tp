"""
dashboard streamlit : couche insights
lit UNIQUEMENT le data warehouse postgres (jamais le data lake), avec un user en lecture seule.

onglets :
  - temps réel        : dw.live_quakes (alimentée par le streaming), rafraîchie toutes les 60 s
  - analyses          : le gold en étoile (fact_earthquake + dimensions) et les agrégats pré-calculés
"""
import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

st.set_page_config(page_title="séismes USGS", page_icon="🌍", layout="wide")

PG = dict(host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")), dbname=os.getenv("PG_DB", "quakes"),
          user=os.getenv("PG_USER", "dashboard_reader"), password=os.getenv("PG_PASSWORD", "dashboard_reader"))

MAG_ORDER = ["micro", "mineur", "léger", "modéré", "fort", "majeur", "grand"]


@st.cache_data(ttl=30)   # cache 30 s : 10 onglets ouverts ne font pas 10 requêtes
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = psycopg2.connect(**PG)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or None)
            return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    finally:
        conn.close()


def table_exists(name: str) -> bool:
    return bool(query("SELECT to_regclass(%s) IS NOT NULL AS ok", (f"dw.{name}",)).ok[0])


st.title("séismes dans le monde")
st.caption("source : USGS earthquake catalog · pipeline kafka → spark → HDFS → postgres")

tab_live, tab_analyse = st.tabs(["temps réel", "analyses historiques"])

# =========================================================================== temps réel
with tab_live:
    @st.fragment(run_every=60)          # ce bloc se rafraîchit seul toutes les 60 s
    def live():
        df = query("""
            SELECT event_id, event_time, mag, place, region, lon, lat, depth_km, status, tsunami, url
            FROM dw.live_quakes
            WHERE event_time > now() - interval '24 hours' AND event_type = 'earthquake'
            ORDER BY event_time DESC
        """)
        if df.empty:
            st.info("aucun séisme en base pour l'instant : le producer kafka et le job streaming tournent ?")
            return
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("séismes (24 h)", len(df))
        c2.metric("magnitude max", f"{df.mag.max():.1f}")
        c3.metric("M4.5+", int((df.mag >= 4.5).sum()))
        c4.metric("dernier", pd.to_datetime(df.event_time.max()).strftime("%H:%M UTC"))

        df["taille"] = (df.mag.clip(lower=0) + 1) ** 2   # aire du point ~ magnitude
        fig = px.scatter_geo(df, lat="lat", lon="lon", size="taille", color="mag",
                             color_continuous_scale="YlOrRd", hover_name="place",
                             hover_data={"mag": ":.1f", "depth_km": ":.0f", "taille": False, "lat": False, "lon": False},
                             projection="natural earth", height=550)
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("les 20 plus forts des dernières 24 h")
        st.dataframe(df.nlargest(20, "mag")[["event_time", "mag", "place", "depth_km", "status", "url"]],
                     hide_index=True, use_container_width=True,
                     column_config={"url": st.column_config.LinkColumn("fiche USGS")})
    live()

# =========================================================================== analyses
with tab_analyse:
    span = query("SELECT min(year) AS lo, max(year) AS hi FROM dw.dim_date")
    if span.lo.isna()[0]:
        st.info("le gold n'est pas encore chargé : lance jobs/batch_gold.py")
    else:
        lo, hi = int(span.lo[0]), int(span.hi[0])
        f1, f2 = st.columns([2, 1])
        years = f1.slider("période", lo, hi, (lo, hi)) if hi > lo else (lo, hi)
        min_mag = f2.slider("magnitude minimum", 0.0, 7.0, 2.5, 0.5,
                            help="sous M2.5 le catalogue est dominé par les USA (bien plus de capteurs) : "
                                 "ça fausse les comparaisons entre régions")
        p = (years[0], years[1], min_mag)
        # le filtre commun : jointure fait -> dimension date (c'est tout l'intérêt de l'étoile)
        WHERE = "d.year BETWEEN %s AND %s AND f.mag >= %s"

        # ---- chiffres clés
        k = query(f"""
            SELECT count(*) AS n, count(*) FILTER (WHERE f.mag >= 6) AS n6,
                   count(*) FILTER (WHERE f.tsunami) AS n_tsu, max(f.mag) AS mmax
            FROM dw.fact_earthquake f JOIN dw.dim_date d USING (date_key) WHERE {WHERE}""", p)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("séismes", f"{int(k.n[0]):,}".replace(",", " "))
        c2.metric("M6+", f"{int(k.n6[0]):,}".replace(",", " "))
        c3.metric("avec alerte tsunami", f"{int(k.n_tsu[0]):,}".replace(",", " "))
        c4.metric("magnitude max", f"{k.mmax[0]:.1f}" if k.mmax[0] is not None else "-")

        # ---- évolution annuelle par classe de magnitude
        st.subheader("combien de séismes par an ?")
        evo = query(f"""
            SELECT d.year, m.label, count(*) AS n
            FROM dw.fact_earthquake f
            JOIN dw.dim_date d USING (date_key) JOIN dw.dim_magnitude m USING (mag_class_key)
            WHERE {WHERE} GROUP BY d.year, m.label, m.mag_class_key ORDER BY d.year, m.mag_class_key""", p)
        fig = px.bar(evo, x="year", y="n", color="label", category_orders={"label": MAG_ORDER},
                     color_discrete_sequence=px.colors.sequential.YlOrRd[1:], labels={"n": "séismes", "year": "", "label": "classe"})
        fig.update_layout(height=380, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

        # ---- heatmap région × année
        st.subheader("où et quand ? les 15 régions les plus actives")
        heat = query(f"""
            WITH top AS (
                SELECT f.region_key FROM dw.fact_earthquake f JOIN dw.dim_date d USING (date_key)
                WHERE {WHERE} GROUP BY f.region_key ORDER BY count(*) DESC LIMIT 15)
            SELECT r.region, d.year, count(*) AS n
            FROM dw.fact_earthquake f
            JOIN top USING (region_key) JOIN dw.dim_region r USING (region_key) JOIN dw.dim_date d USING (date_key)
            WHERE {WHERE} GROUP BY r.region, d.year""", p + p)
        if not heat.empty:
            pivot = heat.pivot(index="region", columns="year", values="n").fillna(0)
            pivot = pivot.loc[pivot.sum(axis=1).sort_values().index]      # la plus active en haut
            fig = px.imshow(pivot, aspect="auto", color_continuous_scale="YlOrRd",
                            labels=dict(x="", y="", color="séismes"))
            fig.update_layout(height=480, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)

        col_a, col_b = st.columns(2)

        # ---- gutenberg-richter
        with col_a:
            st.subheader("la loi de gutenberg-richter")
            gr = query("SELECT mag_bin, n_quakes FROM dw.agg_mag_distribution ORDER BY mag_bin")
            gr = gr[gr.n_quakes > 0].copy()
            # N(>= M) : nombre cumulé de séismes de magnitude au moins M
            gr["n_cum"] = gr.n_quakes[::-1].cumsum()[::-1]
            # magnitude de complétude Mc : le bin le plus peuplé (méthode "max curvature").
            # en dessous, le catalogue rate des séismes trop petits pour être détectés partout
            mc = float(gr.loc[gr.n_quakes.idxmax(), "mag_bin"])
            fit = gr[(gr.mag_bin >= mc) & (gr.n_cum >= 10)]
            b, a = np.polyfit(fit.mag_bin, np.log10(fit.n_cum), 1)
            fig = go.Figure()
            fig.add_scatter(x=gr.mag_bin, y=gr.n_cum, mode="markers", name="observé", marker=dict(color="#D85A30"))
            fig.add_scatter(x=fit.mag_bin, y=10 ** (a + b * fit.mag_bin), mode="lines", name=f"log₁₀N = {a:.2f} − {-b:.2f}·M",
                            line=dict(color="#444441", dash="dash"))
            fig.update_yaxes(type="log", title="N séismes de magnitude ≥ M")
            fig.update_xaxes(title="magnitude M")
            fig.update_layout(height=420, margin=dict(t=10), legend=dict(x=0.45, y=0.95))
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"b = {-b:.2f} (attendu ≈ 1 en sismologie) · complétude Mc ≈ {mc:.1f} · "
                       "chaque point de magnitude en plus = environ 10 fois moins de séismes")

        # ---- profondeur vs magnitude
        with col_b:
            st.subheader("profondeur vs magnitude")
            dep = query(f"""
                SELECT f.mag, f.depth_km, dp.label AS profondeur, r.region
                FROM dw.fact_earthquake f
                JOIN dw.dim_date d USING (date_key) JOIN dw.dim_depth dp USING (depth_class_key)
                JOIN dw.dim_region r USING (region_key)
                WHERE {WHERE} AND f.mag >= 4.5
                ORDER BY random() LIMIT 5000""", p)
            fig = px.scatter(dep, x="mag", y="depth_km", color="profondeur", opacity=0.5, hover_data=["region"],
                             category_orders={"profondeur": ["superficiel", "intermédiaire", "profond"]},
                             color_discrete_sequence=["#EF9F27", "#D85A30", "#712B13"],
                             labels={"mag": "magnitude", "depth_km": "profondeur (km)"})
            fig.update_yaxes(autorange="reversed")      # la profondeur descend
            fig.update_layout(height=420, margin=dict(t=10))
            st.plotly_chart(fig, use_container_width=True)
            st.caption("échantillon de 5 000 séismes M4.5+ · les séismes profonds (> 300 km) n'existent "
                       "que dans les zones de subduction")

        # ---- top régions
        st.subheader("top 15 régions")
        top = query(f"""
            SELECT r.region, count(*) AS n, round(avg(f.mag)::numeric, 2) AS mag_moy, max(f.mag) AS mag_max
            FROM dw.fact_earthquake f JOIN dw.dim_region r USING (region_key) JOIN dw.dim_date d USING (date_key)
            WHERE {WHERE} GROUP BY r.region ORDER BY n DESC LIMIT 15""", p)
        fig = px.bar(top[::-1], x="n", y="region", orientation="h", color="mag_moy",
                     color_continuous_scale="YlOrRd", labels={"n": "séismes", "region": "", "mag_moy": "mag. moy."})
        fig.update_layout(height=480, margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)
