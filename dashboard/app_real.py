import json
import os
import subprocess
import sys
import time
from collections import deque

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import xgboost as xgb
from deltalake import DeltaTable
from sklearn.metrics import average_precision_score, precision_recall_curve

st.set_page_config(page_title="Fraude : données réelles", page_icon="🛡️", layout="wide")
RED, CYAN, AMBER = "#ff4d6d", "#22d3ee", "#fbbf24"
FEATURES = [f"V{i}" for i in range(1, 29)] + ["Amount"]

st.markdown("""
<style>
#MainMenu, footer, header [data-testid="stToolbar"] {visibility: hidden;}
.block-container {padding-top: 1.5rem; max-width: 1400px;}
[data-testid="stMetric"] {
    background: linear-gradient(145deg, #121a2b, #0f1626);
    border: 1px solid #1f2b45; border-left: 4px solid #ff4d6d;
    border-radius: 12px; padding: 14px 18px; box-shadow: 0 4px 14px rgba(0,0,0,.35);
}
[data-testid="stMetricLabel"] {color: #8da2c7; font-size: .85rem;}
[data-testid="stMetricValue"] {font-size: 1.7rem; font-weight: 700;}
.stTabs [data-baseweb="tab"] {background: #121a2b; border-radius: 10px 10px 0 0; padding: 8px 18px;}
.stTabs [aria-selected="true"] {background: #1c2742; border-bottom: 2px solid #ff4d6d;}
h1.hero {font-size: 2rem; margin: 0 0 .3rem 0;
    background: linear-gradient(90deg, #ff4d6d, #22d3ee);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;}
.tag {display: inline-block; background: #22d3ee22; border: 1px solid #22d3ee; border-radius: 999px;
    padding: 2px 12px; font-size: .75rem; font-weight: 700; margin-left: 10px;
    -webkit-text-fill-color: #22d3ee;}
section[data-testid="stSidebar"] {background: #0f1626; border-right: 1px solid #1f2b45;}
</style>
""", unsafe_allow_html=True)


def style(fig, h=300, legend=False):
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=10, r=10, t=10, b=10),
                      height=h, showlegend=legend, font=dict(color="#c7d2e8"))
    fig.update_xaxes(gridcolor="#1f2b45")
    fig.update_yaxes(gridcolor="#1f2b45")
    return fig


@st.cache_data
def load():
    df = DeltaTable("data/silver/transactions_real").to_pandas()
    df = df.sort_values("row_id").reset_index(drop=True)
    df["classe"] = np.where(df.is_fraud == 1, "Fraude", "Normale")
    return df


@st.cache_resource
def get_model():
    m = xgb.XGBClassifier()
    m.load_model("models/xgb_real.json")
    return m


@st.cache_data
def test_set():
    df = load()
    t = df.iloc[int(len(df) * 0.8):].copy()
    t["proba"] = get_model().predict_proba(t[FEATURES])[:, 1]
    return t


def conf(y, p, t):
    f = p >= t
    return (int((f & (y == 1)).sum()), int((f & (y == 0)).sum()),
            int((~f & (y == 1)).sum()), int((~f & (y == 0)).sum()))


def prf(tp, fp, fn):
    pr = tp / (tp + fp) if tp + fp else 0.0
    rc = tp / (tp + fn) if tp + fn else 0.0
    return pr, rc, (2 * pr * rc / (pr + rc) if pr + rc else 0.0)


DF, T = load(), test_set()
y, p, a = T.is_fraud.values, T.proba.values, T.Amount.values

st.sidebar.title("🛡️ Données réelles")
st.sidebar.caption("Jeu ULB / Worldline, 284 807 transactions")
thr = st.sidebar.slider("Seuil de décision", 0.01, 0.99, 0.50, 0.01)
cost = st.sidebar.number_input("Coût d'une alerte (euros)", 0.0, 500.0, 5.0, 1.0)
st.sidebar.info("Le modèle est entraîné sur les 80 % les plus anciens. "
                "Les onglets Modèle, Coût et Rejeu utilisent les 20 % les plus récents (test).")

st.markdown('<h1 class="hero">Détection de fraude sur données réelles'
            '<span class="tag">ULB</span></h1>', unsafe_allow_html=True)
tabs = st.tabs(["📊 Vue d'ensemble", "🔎 Exploration", "🎯 Modèle", "💶 Coût",
                "📡 Flux temps réel", "⚙️ Pipeline"])

with tabs[0]:
    nf = int(DF.is_fraud.sum())
    k = st.columns(4)
    k[0].metric("Transactions", f"{len(DF):,}")
    k[1].metric("Fraudes", f"{nf:,}")
    k[2].metric("Taux de fraude", f"{100 * nf / len(DF):.3f} %")
    k[3].metric("Montant moyen (fraude / normale)",
                f"{DF[DF.is_fraud == 1].Amount.mean():.0f} / {DF[DF.is_fraud == 0].Amount.mean():.0f}")
    st.write("")
    c1, c2 = st.columns(2)
    c1.subheader("Fraudes dans le temps (ordre des lignes)")
    bins = pd.cut(DF.row_id, 40, labels=False)
    fr = DF.groupby(bins).is_fraud.sum().reset_index()
    fr.columns = ["période", "fraudes"]
    c1.plotly_chart(style(px.bar(fr, x="période", y="fraudes", color_discrete_sequence=[RED])),
                    width="stretch", key="ov_time")
    c2.subheader("Distribution des montants")
    samp = pd.concat([DF[DF.is_fraud == 1], DF[DF.is_fraud == 0].sample(20000, random_state=1)])
    fig = px.histogram(samp, x="Amount", color="classe", nbins=60, log_y=True, barmode="overlay",
                       opacity=0.7, color_discrete_map={"Fraude": RED, "Normale": CYAN})
    c2.plotly_chart(style(fig, legend=True), width="stretch", key="ov_amt")
    st.subheader("Statistiques des montants par classe")
    st.dataframe(DF.groupby("classe").Amount.describe().round(2), width="stretch")

with tabs[1]:
    c1, c2 = st.columns(2)
    c1.subheader("Variables les plus liées à la fraude")
    cor = DF[FEATURES].corrwith(DF.is_fraud).abs().sort_values().tail(15).reset_index()
    cor.columns = ["variable", "corrélation absolue"]
    c1.plotly_chart(style(px.bar(cor, x="corrélation absolue", y="variable", orientation="h",
                                 color_discrete_sequence=[AMBER]), 380), width="stretch", key="ex_cor")
    c2.subheader("Distribution d'une variable")
    var = c2.selectbox("Variable", FEATURES, index=13)
    s = pd.concat([DF[DF.is_fraud == 1], DF[DF.is_fraud == 0].sample(20000, random_state=2)])
    fig = px.histogram(s, x=var, color="classe", nbins=60, histnorm="probability density",
                       barmode="overlay", opacity=0.7,
                       color_discrete_map={"Fraude": RED, "Normale": CYAN})
    c2.plotly_chart(style(fig, 330, legend=True), width="stretch", key="ex_hist")
    st.subheader("Deux variables à la fois")
    a1, a2 = st.columns(2)
    vx = a1.selectbox("Axe X", FEATURES, index=13)
    vy = a2.selectbox("Axe Y", FEATURES, index=9)
    s2 = pd.concat([DF[DF.is_fraud == 1], DF[DF.is_fraud == 0].sample(3000, random_state=3)])
    fig = px.scatter(s2, x=vx, y=vy, color="classe", opacity=0.6,
                     color_discrete_map={"Fraude": RED, "Normale": CYAN})
    st.plotly_chart(style(fig, 380, legend=True), width="stretch", key="ex_sc")

with tabs[2]:
    tp, fp, fn, tn = conf(y, p, thr)
    pr, rc, f1 = prf(tp, fp, fn)
    k = st.columns(5)
    k[0].metric("Précision", f"{pr:.3f}")
    k[1].metric("Rappel", f"{rc:.3f}")
    k[2].metric("F1", f"{f1:.3f}")
    k[3].metric("Fraudes détectées", f"{tp} / {tp + fn}")
    k[4].metric("Fausses alertes", fp)
    st.write("")
    c1, c2 = st.columns(2)
    c1.subheader("Matrice de confusion (test)")
    m = pd.DataFrame([[tn, fp], [fn, tp]], index=["Normale", "Fraude"],
                     columns=["Prédit normale", "Prédit fraude"])
    fig = px.imshow(m, text_auto=True, color_continuous_scale="Reds", aspect="auto")
    fig.update_layout(coloraxis_showscale=False)
    c1.plotly_chart(style(fig, 320), width="stretch", key="md_cm")
    c2.subheader("Courbe précision / rappel")
    prec, rec, _ = precision_recall_curve(y, p)
    fig = go.Figure(go.Scatter(x=rec, y=prec, mode="lines", line=dict(color=CYAN, width=3)))
    fig.add_trace(go.Scatter(x=[rc], y=[pr], mode="markers", marker=dict(color=RED, size=14)))
    fig.update_xaxes(title="Rappel")
    fig.update_yaxes(title="Précision")
    c2.plotly_chart(style(fig, 320), width="stretch", key="md_pr")
    c2.caption(f"PR-AUC sur le test : {average_precision_score(y, p):.3f}. "
               "Le point rouge est le seuil choisi dans la barre latérale.")
    st.subheader("Importance des variables")
    imp = pd.Series(get_model().feature_importances_, index=FEATURES).sort_values().tail(12).reset_index()
    imp.columns = ["variable", "importance"]
    st.plotly_chart(style(px.bar(imp, x="importance", y="variable", orientation="h",
                                 color_discrete_sequence=[RED]), 340), width="stretch", key="md_imp")

with tabs[3]:
    ths = np.round(np.arange(0.01, 1.0, 0.01), 2)
    costs = np.array([cost * (p >= t).sum() + a[(p < t) & (y == 1)].sum() for t in ths])
    base = a[y == 1].sum()
    best = ths[costs.argmin()]
    cur = cost * (p >= thr).sum() + a[(p < thr) & (y == 1)].sum()
    k = st.columns(4)
    k[0].metric("Coût sans modèle", f"{base:,.0f} €")
    k[1].metric("Coût au seuil choisi", f"{cur:,.0f} €")
    k[2].metric("Économie", f"{100 * (1 - cur / base):.0f} %")
    k[3].metric("Seuil le moins coûteux", f"{best}")
    st.subheader("Coût total selon le seuil (test)")
    fig = go.Figure(go.Scatter(x=ths, y=costs, mode="lines", line=dict(color=AMBER, width=3)))
    fig.add_hline(y=base, line_dash="dash", line_color=RED, annotation_text="sans modèle")
    fig.add_vline(x=thr, line_dash="dot", line_color=CYAN, annotation_text="seuil choisi")
    fig.update_xaxes(title="Seuil")
    fig.update_yaxes(title="Coût (euros)")
    st.plotly_chart(style(fig, 360), width="stretch", key="co_curve")
    st.warning("Ce seuil est calculé sur le test, donc il est flatteur. Avec seulement 75 fraudes, "
               "il n'est pas stable : en validation, le seuil « optimal » s'est avéré moins bon "
               "que 0,5 (voir le README).")

def simulated():
    st.write("Les transactions les plus récentes sont rejouées par paquets et scorées en local, "
             "avec le seuil de la barre latérale. Aucun Kafka n'est nécessaire.")
    c1, c2 = st.columns(2)
    n = c1.slider("Nombre de transactions", 1000, 30000, 10000, 1000)
    batch = c2.slider("Transactions par paquet", 100, 1000, 250, 50)
    if st.button("▶ Lancer le rejeu simulé"):
        ph = [c.empty() for c in st.columns(5)]
        chart, table = st.empty(), st.empty()
        TP = FP = FN = TN = 0
        hist, last_df = [], pd.DataFrame()
        rows = T.iloc[:n]
        for i in range(0, len(rows), batch):
            b = rows.iloc[i:i + batch]
            btp, bfp, bfn, btn = conf(b.is_fraud.values, b.proba.values, thr)
            TP, FP, FN, TN = TP + btp, FP + bfp, FN + bfn, TN + btn
            done = i + len(b)
            hist.append({"transactions": done, "alertes": TP + FP,
                         "fraudes détectées": TP, "fraudes manquées": FN})
            hit = b[b.proba >= thr][["row_id", "Amount", "proba", "classe"]]
            last_df = pd.concat([last_df, hit]).tail(12)
            ph[0].metric("Traitées", f"{done:,}")
            ph[1].metric("Alertes", TP + FP)
            ph[2].metric("Fraudes détectées", TP)
            ph[3].metric("Fraudes manquées", FN)
            ph[4].metric("Fausses alertes", FP)
            h = pd.DataFrame(hist).melt("transactions", var_name="série", value_name="n")
            fig = px.line(h, x="transactions", y="n", color="série",
                          color_discrete_sequence=[AMBER, CYAN, RED])
            chart.plotly_chart(style(fig, 300, legend=True), width="stretch", key=f"rj_{i}")
            table.dataframe(last_df.iloc[::-1], hide_index=True, width="stretch")
            time.sleep(0.25)
        st.success(f"Rejeu terminé : {TP} fraudes détectées, {FN} manquées, {FP} fausses alertes.")


@st.cache_resource
def live_state():
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": "localhost:9092",
                  "group.id": f"dash-real-{int(time.time())}",
                  "auto.offset.reset": "earliest"})
    c.subscribe(["fraud_alerts_real"])
    return {"c": c, "run": 0, "alerts": deque(maxlen=2000), "stats": [], "procs": {}, "t0": 0}


def live_poll(L):
    for m in L["c"].consume(num_messages=2000, timeout=0.3):
        if m.error():
            continue
        d = json.loads(m.value())
        if d["run_id"] > L["run"]:
            L["run"] = d["run_id"]
            L["alerts"].clear()
            L["stats"] = []
        if d["run_id"] < L["run"]:
            continue
        if d["type"] == "stats":
            L["stats"] = (L["stats"] + [d])[-5000:]
        else:
            L["alerts"].append(d)


def running(L, name):
    p = L["procs"].get(name)
    return p is not None and p.poll() is None


def launch(L, name, args):
    if not running(L, name):
        L["procs"][name] = subprocess.Popen([sys.executable, *args],
                                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@st.fragment(run_every=2)
def live_view():
    L = live_state()
    live_poll(L)
    if not L["stats"]:
        st.info("En attente du scorer et du rejeu (Kafka doit tourner).")
        return
    s = L["stats"][-1]
    pr, rc, _ = prf(s["tp"], s["fp"], s["fn"])
    k = st.columns(6)
    k[0].metric("Traitées", f"{s['processed']:,}")
    k[1].metric("Alertes", s["tp"] + s["fp"])
    k[2].metric("Fraudes détectées", s["tp"])
    k[3].metric("Fraudes manquées", s["fn"])
    k[4].metric("Fausses alertes", s["fp"])
    k[5].metric("Latence (dernier paquet)", f"{s['lat_win']:.0f} ms")
    st.caption(f"Précision en cours : {pr:.3f}, rappel en cours : {rc:.3f}")
    h = pd.DataFrame(L["stats"])
    h["alertes"] = h.tp + h.fp
    a, b = st.columns(2)
    a.subheader("Cumul depuis le début du rejeu")
    m = h.rename(columns={"tp": "fraudes détectées", "fn": "fraudes manquées"})
    m = m.melt("processed", ["alertes", "fraudes détectées", "fraudes manquées"],
               var_name="série", value_name="n")
    fig = px.line(m, x="processed", y="n", color="série",
                  color_discrete_sequence=[AMBER, CYAN, RED])
    a.plotly_chart(style(fig, 300, legend=True), width="stretch", key="lv_cum")
    b.subheader("Latence de bout en bout (ms)")
    fig = px.line(h, x="processed", y="lat_win", color_discrete_sequence=[CYAN])
    b.plotly_chart(style(fig, 300), width="stretch", key="lv_lat")
    st.subheader("Dernières alertes")
    al = pd.DataFrame(list(L["alerts"])[-15:][::-1])
    if not al.empty:
        al["classe"] = al.is_fraud.map({1: "Fraude", 0: "Fausse alerte"})
        st.dataframe(al[["row_id", "Amount", "score", "classe", "latency_ms"]],
                     hide_index=True, width="stretch",
                     column_config={"score": st.column_config.ProgressColumn(
                         "Score", min_value=0, max_value=1, format="%.2f")})


with tabs[4]:
    local = os.path.exists("data/silver/transactions_real")
    mode = st.radio("Mode", ["Kafka (temps réel)", "Simulé (sans Kafka)"],
                    index=0 if local else 1, horizontal=True)
    if mode.startswith("Simulé"):
        simulated()
    else:
        L = live_state()
        st.write("Flux : rejeu du fichier, topic `transactions_real`, scorer, topic "
                 "`fraud_alerts_real`, ce dashboard. Le rejeu commence à la ligne 227 845 : "
                 "ce sont les transactions les plus récentes, que le modèle n'a jamais vues.")
        c1, c2, c3 = st.columns(3)
        rate = c1.slider("Débit (transactions/s)", 50, 1000, 300, 50)
        nb = c2.slider("Nombre de transactions", 1000, 50000, 10000, 1000)
        if c3.button("Arrêter le scorer") and running(L, "scorer"):
            L["procs"]["scorer"].terminate()
        b1, b2 = st.columns(2)
        if b1.button("1. Démarrer le scorer"):
            launch(L, "scorer", ["ml/score_stream_real.py", "--threshold", str(thr)])
            L["t0"] = time.time()
        if b2.button("2. Lancer le rejeu"):
            if not running(L, "scorer"):
                st.warning("Démarre d'abord le scorer.")
            elif time.time() - L["t0"] < 15:
                st.warning("Attends 15 secondes après le démarrage du scorer.")
            else:
                launch(L, "replay", ["generator/replay_ulb.py", "--rate", str(rate),
                                     "--start", "227845", "--limit", str(nb)])
        st.caption(f"Scorer : {'en marche' if running(L, 'scorer') else 'arrêté'} | "
                   f"Rejeu : {'en cours' if running(L, 'replay') else 'arrêté'}")
        live_view()
with tabs[5]:
    def kafka_count():
        try:
            from confluent_kafka import Consumer, TopicPartition
            c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "ui-real"})
            n = sum(c.get_watermark_offsets(TopicPartition("transactions_real", i), timeout=3)[1]
                    for i in range(3))
            c.close()
            return n
        except Exception:
            return None

    def delta_count(path):
        try:
            return DeltaTable(path).to_pyarrow_dataset().count_rows()
        except Exception:
            return None

    def fmt(v):
        return f"{v:,}" if v is not None else "n/a"

    k = st.columns(3)
    k[0].metric("Kafka (transactions_real)", fmt(kafka_count()))
    k[1].metric("Bronze réel", fmt(delta_count("data/bronze/transactions_real")))
    k[2].metric("Silver réel", fmt(delta_count("data/silver/transactions_real")))
    st.caption("« n/a » pour Kafka veut dire que Docker ou Kafka n'est pas démarré.")
    st.subheader("Dernier modèle réel enregistré (MLflow)")
    try:
        from mlflow.tracking import MlflowClient
        cl = MlflowClient(tracking_uri="sqlite:///mlflow.db")
        exp = cl.get_experiment_by_name("fraud-detection-real")
        mm = cl.search_runs([exp.experiment_id], order_by=["attributes.start_time DESC"],
                            max_results=1)[0].data.metrics
        k = st.columns(4)
        for col, key in zip(k, ["precision", "recall", "f1", "pr_auc"]):
            col.metric(key, f"{mm.get(key, float('nan')):.3f}")
    except Exception:
        st.info("Aucune métrique MLflow trouvée (lance le dashboard depuis la racine du projet).")