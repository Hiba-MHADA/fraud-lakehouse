import json
import subprocess
import sys
import threading
import time
from collections import deque

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from confluent_kafka import Consumer, TopicPartition

st.set_page_config(page_title="Fraud Lakehouse", page_icon="🛡️", layout="wide")

RED, CYAN, AMBER = "#ff4d6d", "#22d3ee", "#fbbf24"

st.markdown("""
<style>
#MainMenu, footer, header [data-testid="stToolbar"] {visibility: hidden;}
.block-container {padding-top: 1.5rem; max-width: 1400px;}
[data-testid="stMetric"] {
    background: linear-gradient(145deg, #121a2b, #0f1626);
    border: 1px solid #1f2b45; border-left: 4px solid #ff4d6d;
    border-radius: 12px; padding: 14px 18px;
    box-shadow: 0 4px 14px rgba(0,0,0,.35);
}
[data-testid="stMetricLabel"] {color: #8da2c7; font-size: .85rem;}
[data-testid="stMetricValue"] {font-size: 1.8rem; font-weight: 700;}
.stTabs [data-baseweb="tab-list"] {gap: 6px;}
.stTabs [data-baseweb="tab"] {
    background: #121a2b; border-radius: 10px 10px 0 0; padding: 8px 18px;
}
.stTabs [aria-selected="true"] {background: #1c2742; border-bottom: 2px solid #ff4d6d;}
h1.hero {
    font-size: 2.1rem; margin: 0 0 .2rem 0;
    background: linear-gradient(90deg, #ff4d6d, #22d3ee);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.live {display: inline-block; background: #ff4d6d22; color: #ff4d6d;
    border: 1px solid #ff4d6d; border-radius: 999px; padding: 2px 12px;
    font-size: .75rem; font-weight: 700; margin-left: 10px;
    -webkit-text-fill-color: #ff4d6d;}
section[data-testid="stSidebar"] {background: #0f1626; border-right: 1px solid #1f2b45;}
</style>
""", unsafe_allow_html=True)

COORDS = {
    "MA": (-7.6, 33.6), "FR": (2.3, 48.9), "ES": (-3.7, 40.4), "DE": (13.4, 52.5),
    "US": (-77.0, 38.9), "GB": (-0.1, 51.5), "IT": (12.5, 41.9), "AE": (55.3, 25.2),
}
COUNTRIES = list(COORDS)
MERCHANTS = ["atm", "electronics", "fuel", "grocery", "online_shop", "restaurant", "travel"]


def style(fig, h=300, legend=False):
    fig.update_layout(
        template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=10, b=10), height=h, showlegend=legend,
        font=dict(color="#c7d2e8"))
    fig.update_xaxes(gridcolor="#1f2b45")
    fig.update_yaxes(gridcolor="#1f2b45")
    return fig


@st.cache_resource
def get_state():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dashboard-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["fraud_alerts"])
    return {"consumer": consumer, "buffer": deque(maxlen=5000),
            "lock": threading.Lock(), "procs": {}}


S = get_state()


def poll():
    if st.session_state.get("pause"):
        return
    with S["lock"]:
        for m in S["consumer"].consume(num_messages=1000, timeout=0.3):
            if not m.error():
                S["buffer"].append(json.loads(m.value()))


def get_df():
    poll()
    with S["lock"]:
        rows = list(S["buffer"])
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["event_time"] = pd.to_datetime(df["event_time"])
    return df[
        (df.score >= st.session_state.get("min_score", 0.5))
        & df.country.isin(st.session_state.get("countries", COUNTRIES))
        & df.merchant_category.isin(st.session_state.get("merchants", MERCHANTS))
    ]


def topic_count(topic):
    total = 0
    with S["lock"]:
        for p in range(3):
            lo, hi = S["consumer"].get_watermark_offsets(
                TopicPartition(topic, p), timeout=3, cached=False)
            total += hi - lo
    return total


def is_running(name):
    p = S["procs"].get(name)
    return p is not None and p.poll() is None


def start(name, args):
    if not is_running(name):
        S["procs"][name] = subprocess.Popen(
            [sys.executable, *args], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def stop(name):
    if is_running(name):
        S["procs"][name].terminate()


@st.cache_data(ttl=60)
def delta_counts():
    from deltalake import DeltaTable
    out = {}
    for name, path in [("Bronze", "data/bronze/transactions"),
                       ("Silver", "data/silver/transactions"),
                       ("Gold", "data/gold/features")]:
        try:
            out[name] = DeltaTable(path).to_pyarrow_dataset().count_rows()
        except Exception:
            out[name] = None
    return out


@st.cache_data(ttl=300)
def model_metrics():
    try:
        from mlflow.tracking import MlflowClient
        c = MlflowClient(tracking_uri="sqlite:///mlflow.db")
        exp = c.get_experiment_by_name("fraud-detection")
        run = c.search_runs([exp.experiment_id],
                            order_by=["attributes.start_time DESC"], max_results=1)[0]
        return run.data.metrics
    except Exception:
        return {}


# ---------- Barre latérale ----------
st.sidebar.title("🛡️ Fraud Lakehouse")
st.sidebar.subheader("Filtres")
st.sidebar.toggle("Pause", key="pause")
st.sidebar.slider("Score minimum", 0.5, 1.0, 0.5, 0.01, key="min_score")
st.sidebar.multiselect("Pays de la transaction", COUNTRIES, default=COUNTRIES, key="countries")
st.sidebar.multiselect("Commerçants", MERCHANTS, default=MERCHANTS, key="merchants")

st.sidebar.subheader("Contrôles")
if is_running("scorer"):
    st.sidebar.success("Scorer : en marche")
    if st.sidebar.button("Arrêter le scorer"):
        stop("scorer")
        st.rerun()
else:
    st.sidebar.warning("Scorer : arrêté")
    if st.sidebar.button("Démarrer le scorer"):
        start("scorer", ["ml/score_stream.py"])
        st.rerun()

rate = st.sidebar.slider("Débit du générateur (événements/s)", 50, 1000, 200, 50)
duration = st.sidebar.slider("Durée (secondes)", 10, 300, 60, 10)
if st.sidebar.button("Lancer le générateur"):
    start("generator", ["generator/producer.py", "--rate", str(rate),
                        "--duration", str(duration)])
st.sidebar.caption("Générateur : " + ("en cours" if is_running("generator") else "arrêté"))

st.markdown('<h1 class="hero">Détection de fraude en temps réel'
            '<span class="live">● LIVE</span></h1>', unsafe_allow_html=True)
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📈 Temps réel", "🔬 Analyse", "🗺️ Carte", "🕵️ Enquête", "⚙️ Pipeline"])


@st.fragment(run_every=2)
def realtime():
    df = get_df()
    if df.empty:
        st.info("En attente d'alertes (démarre le scorer et le générateur dans la barre latérale).")
        return
    last_min = int((df.event_time > df.event_time.max() - pd.Timedelta(minutes=1)).sum())
    k = st.columns(5)
    k[0].metric("Alertes", f"{len(df):,}")
    k[1].metric("Montant suspect", f"{df.amount.sum():,.0f}")
    k[2].metric("Latence moyenne", f"{df.latency_ms.mean():.0f} ms")
    k[3].metric("Score moyen", f"{df.score.mean():.2f}")
    k[4].metric("Dernière minute", last_min)
    st.write("")

    a, b = st.columns([3, 2])
    a.subheader("Alertes par 30 secondes")
    ts = df.set_index("event_time").resample("30s").size().reset_index(name="alertes")
    fig = px.area(ts, x="event_time", y="alertes", color_discrete_sequence=[RED])
    a.plotly_chart(style(fig, 280), width="stretch", key="rt_area")

    b.subheader("Répartition par commerçant")
    m = df.merchant_category.value_counts().reset_index()
    m.columns = ["commerçant", "n"]
    fig = px.pie(m, names="commerçant", values="n", hole=0.6)
    b.plotly_chart(style(fig, 280, legend=True), width="stretch", key="rt_pie")

    c, d = st.columns(2)
    c.subheader("Latence moyenne (ms)")
    lat = df.set_index("event_time")["latency_ms"].resample("30s").mean().reset_index()
    fig = px.line(lat, x="event_time", y="latency_ms", color_discrete_sequence=[CYAN])
    c.plotly_chart(style(fig, 240), width="stretch", key="rt_lat")
    d.subheader("Pays de la transaction")
    p = df.country.value_counts().reset_index()
    p.columns = ["pays", "n"]
    fig = px.bar(p, x="pays", y="n", color_discrete_sequence=[AMBER])
    d.plotly_chart(style(fig, 240), width="stretch", key="rt_country")

    st.subheader("Dernières alertes")
    cols = ["event_time", "card_id", "amount", "merchant_category",
            "country", "home_country", "score", "latency_ms"]
    st.dataframe(
        df.sort_values("event_time", ascending=False).head(15)[cols],
        hide_index=True, width="stretch",
        column_config={
            "event_time": st.column_config.DatetimeColumn("Heure", format="HH:mm:ss"),
            "amount": st.column_config.NumberColumn("Montant", format="%.2f"),
            "score": st.column_config.ProgressColumn("Score", min_value=0, max_value=1, format="%.2f"),
            "latency_ms": st.column_config.NumberColumn("Latence (ms)", format="%.0f"),
        },
    )


@st.fragment(run_every=5)
def analysis():
    df = get_df()
    if df.empty:
        st.info("Pas encore d'alertes.")
        return
    a, b = st.columns(2)
    a.subheader("Distribution des scores")
    fig = px.histogram(df, x="score", nbins=25, color_discrete_sequence=[RED])
    a.plotly_chart(style(fig, 280), width="stretch", key="an_score")
    b.subheader("Distribution des montants")
    fig = px.histogram(df, x="amount", nbins=30, log_y=True, color_discrete_sequence=[CYAN])
    b.plotly_chart(style(fig, 280), width="stretch", key="an_amount")

    c, d = st.columns(2)
    c.subheader("Top 10 des cartes")
    t = df.card_id.value_counts().head(10).iloc[::-1].reset_index()
    t.columns = ["carte", "alertes"]
    fig = px.bar(t, x="alertes", y="carte", orientation="h", color_discrete_sequence=[AMBER])
    c.plotly_chart(style(fig, 320), width="stretch", key="an_cards")
    d.subheader("Pays d'origine vers pays de la transaction")
    ct = pd.crosstab(df.home_country, df.country)
    fig = px.imshow(ct, text_auto=True, color_continuous_scale="Reds", aspect="auto")
    fig.update_layout(coloraxis_showscale=False)
    d.plotly_chart(style(fig, 320), width="stretch", key="an_heat")


@st.fragment(run_every=5)
def world_map():
    df = get_df()
    if df.empty:
        st.info("Pas encore d'alertes.")
        return
    arcs = df.groupby(["home_country", "country"]).size().reset_index(name="n")
    arcs = arcs[(arcs.home_country != arcs.country)
                & arcs.home_country.isin(COORDS) & arcs.country.isin(COORDS)].copy()
    arcs["src"] = arcs.home_country.map(lambda c: list(COORDS[c]))
    arcs["dst"] = arcs.country.map(lambda c: list(COORDS[c]))
    layer = pdk.Layer(
        "ArcLayer", data=arcs, get_source_position="src", get_target_position="dst",
        get_width="1 + n / 3", get_source_color=[34, 211, 238], get_target_color=[255, 77, 109],
        pickable=True)
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(latitude=30, longitude=10, zoom=1.3),
        tooltip={"text": "{home_country} vers {country} : {n} alertes"}))
    st.caption("Bleu : pays d'origine de la carte. Rouge : pays de la transaction frauduleuse.")


@st.fragment(run_every=5)
def investigation():
    df = get_df()
    if df.empty:
        st.info("Pas encore d'alertes.")
        return
    cards = df.card_id.value_counts()
    choice = st.selectbox("Carte à examiner", cards.index,
                          format_func=lambda c: f"{c} ({cards[c]} alertes)", key="card_choice")
    sub = df[df.card_id == choice].sort_values("event_time", ascending=False)
    k = st.columns(3)
    k[0].metric("Alertes sur cette carte", len(sub))
    k[1].metric("Montant total suspect", f"{sub.amount.sum():,.2f}")
    k[2].metric("Score maximum", f"{sub.score.max():.2f}")
    fig = px.scatter(sub, x="event_time", y="amount", size="score", color="country",
                     hover_data=["merchant_category", "score"])
    st.plotly_chart(style(fig, 260, legend=True), width="stretch", key="inv_scatter")
    st.dataframe(sub[["event_time", "amount", "merchant_category", "country",
                      "home_country", "score"]], hide_index=True, width="stretch")


@st.fragment(run_every=10)
def pipeline():
    tx, al = topic_count("transactions"), topic_count("fraud_alerts")
    k = st.columns(3)
    k[0].metric("Messages dans Kafka (transactions)", f"{tx:,}")
    k[1].metric("Alertes publiées (fraud_alerts)", f"{al:,}")
    k[2].metric("Taux d'alerte", f"{100 * al / tx:.2f} %" if tx else "n/a")

    st.subheader("Lignes dans le lakehouse (Delta Lake)")
    counts = delta_counts()
    k = st.columns(3)
    for col, (name, n) in zip(k, counts.items()):
        col.metric(name, f"{n:,}" if n is not None else "n/a")

    st.subheader("Dernier modèle entraîné (MLflow)")
    mm = model_metrics()
    if mm:
        k = st.columns(4)
        for col, key in zip(k, ["precision", "recall", "f1", "pr_auc"]):
            col.metric(key, f"{mm.get(key, float('nan')):.3f}")
    else:
        st.info("Aucune métrique trouvée (lance le dashboard depuis la racine du projet).")


with tab1:
    realtime()
with tab2:
    analysis()
with tab3:
    world_map()
with tab4:
    investigation()
with tab5:
    pipeline()