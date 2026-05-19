import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import requests
import os

st.set_page_config(page_title="Sentiment + Heart Dashboard", layout="wide", page_icon="📊")

COLOR_MAP_SENT = {"positivo": "#2ecc71", "negativo": "#e74c3c", "neutral": "#3498db"}
COLOR_MAP_HEART = {0: "#2ecc71", 1: "#e74c3c"}
MONGO_URI       = os.environ.get("MONGO_URI",        "mongodb://localhost:27017/sentiment_db")
HEART_MONGO_URI  = os.environ.get("HEART_MONGO_URI", "mongodb://localhost:27017/heart_db")
API_URL          = os.environ.get("API_URL",          "http://localhost:5001")
HEART_API_URL    = os.environ.get("HEART_API_URL",    "http://localhost:5002")
ENRICHED         = os.environ.get("ENRICHED_CSV",     "dataset_final.csv")


# ── Sentiment data ───────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_sentiment():
    client = MongoClient(MONGO_URI)
    docs = list(client.sentiment_db.results.find({}, {"_id": 0}))
    client.close()
    df = pd.DataFrame(docs)
    if "prediction" not in df.columns:
        df["prediction"] = "desconocido"
    if "fecha_proceso" in df.columns:
        df["fecha_proceso"] = pd.to_datetime(df["fecha_proceso"], errors="coerce")
    else:
        df["fecha_proceso"] = pd.NaT
    try:
        enriched = pd.read_csv(ENRICHED, parse_dates=["fecha"]).drop_duplicates(subset="texto")
        df = df.merge(enriched[["texto", "fecha"]], on="texto", how="left")
        df["fecha_proceso"] = df["fecha"].combine_first(df["fecha_proceso"])
        df.drop(columns=["fecha"], inplace=True)
    except Exception:
        pass
    return df


# ── Heart data ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_heart():
    client = MongoClient(HEART_MONGO_URI)
    docs = list(client.heart_db.predictions.find({}, {"_id": 0}))
    client.close()
    df = pd.DataFrame(docs)
    if not df.empty and "fecha_proceso" in df.columns:
        df["fecha_proceso"] = pd.to_datetime(df["fecha_proceso"], errors="coerce")
    return df


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Sentiment Dashboard",
    "📈 Sentiment Stats API",
    "🫀 Heart Dashboard",
    "🔍 Heart Predictions",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Sentiment Dashboard
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.title("🎭 Sentiment Analysis Dashboard")

    try:
        df = load_sentiment()
    except Exception as e:
        st.error(f"No se pudo conectar a MongoDB: {e}")
        st.stop()

    if df.empty:
        st.warning("No hay datos en MongoDB (sentiment_db).")
        st.stop()

    counts = df["prediction"].value_counts()
    total  = len(df)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total",     total)
    c2.metric("Positivos", counts.get("positivo", 0), f"{counts.get('positivo',0)/total*100:.1f}%")
    c3.metric("Negativos", counts.get("negativo", 0), f"{counts.get('negativo',0)/total*100:.1f}%")
    c4.metric("Neutrales", counts.get("neutral",  0), f"{counts.get('neutral', 0)/total*100:.1f}%")

    st.divider()

    cl, cr = st.columns(2)
    with cl:
        st.subheader("Distribución")
        pie = counts.reset_index()
        pie.columns = ["sentimiento", "cantidad"]
        fig = px.pie(pie, values="cantidad", names="sentimiento",
                     color="sentimiento", color_discrete_map=COLOR_MAP_SENT, hole=0.4)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with cr:
        st.subheader("Etiqueta real vs Predicción")
        cmp = df.groupby(["etiqueta", "prediction"]).size().reset_index(name="n")
        fig2 = px.bar(cmp, x="etiqueta", y="n", color="prediction",
                      color_discrete_map=COLOR_MAP_SENT, barmode="group",
                      labels={"etiqueta": "Etiqueta real", "n": "Cantidad", "prediction": "Predicción"})
        fig2.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    try:
        client_m = MongoClient(MONGO_URI)
        metrics = client_m.sentiment_db.metrics.find_one({}, {"_id": 0})
        client_m.close()
        if metrics:
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Accuracy (test set)", f"{metrics.get('accuracy', 0)}%")
            col_b.metric("Train size", metrics.get("train_size", "-"))
            col_c.metric("Test size",  metrics.get("test_size",  "-"))
        else:
            correct = (df["etiqueta"] == df["prediction"]).sum()
            st.metric("Accuracy del modelo", f"{correct/total*100:.1f}%")
    except Exception:
        correct = (df["etiqueta"] == df["prediction"]).sum()
        st.metric("Accuracy del modelo", f"{correct/total*100:.1f}%")
    st.divider()

    st.subheader("Volumen en el tiempo")
    tl = df.set_index("fecha_proceso").resample("7D")["prediction"].count().reset_index()
    tl.columns = ["fecha", "cantidad"]
    fig3 = px.line(tl, x="fecha", y="cantidad", markers=True)
    fig3.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig3, use_container_width=True)
    st.divider()

    st.subheader("Explorar predicciones")
    filtro = st.selectbox("Filtrar", ["Todos", "positivo", "negativo", "neutral"])
    search = st.text_input("Buscar texto")
    view = df.copy()
    if filtro != "Todos":
        view = view[view["prediction"] == filtro]
    if search:
        view = view[view["texto"].str.contains(search, case=False, na=False)]
    cols = [c for c in ["texto", "etiqueta", "prediction", "fecha_proceso"] if c in view.columns]
    st.dataframe(view[cols].rename(columns={"prediction": "predicción", "fecha_proceso": "fecha"}),
                 use_container_width=True, hide_index=True)
    st.caption(f"Mostrando {len(view)} de {total} registros")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Sentiment Stats API
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.title("📈 Stats de la API (Sentiment)")

    if st.button("Actualizar stats"):
        st.cache_data.clear()

    try:
        resp = requests.get(f"{API_URL}/stats", timeout=5)
        resp.raise_for_status()
        stats = resp.json()

        col1, col2 = st.columns(2)
        col1.metric("Total registros", stats.get("total", 0))
        col2.metric("Accuracy", f"{stats.get('accuracy', 0)}%")

        st.subheader("Distribución por clase")
        dist = stats.get("distribucion", {})
        dist_df = pd.DataFrame(list(dist.items()), columns=["sentimiento", "cantidad"])
        fig4 = px.bar(dist_df, x="sentimiento", y="cantidad",
                      color="sentimiento", color_discrete_map=COLOR_MAP_SENT,
                      text="cantidad")
        fig4.update_traces(textposition="outside")
        fig4.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig4, use_container_width=True)

    except Exception as e:
        st.error(f"No se pudo conectar a la API ({API_URL}/stats): {e}")

    st.divider()
    st.subheader("Últimos registros — /sentiments")
    tipo = st.selectbox("Filtrar por tipo", ["", "positivo", "negativo", "neutral"],
                        format_func=lambda x: "Todos" if x == "" else x, key="sent_type")
    try:
        url = f"{API_URL}/sentiments" + (f"?type={tipo}" if tipo else "")
        r2 = requests.get(url, timeout=5)
        r2.raise_for_status()
        sent_df = pd.DataFrame(r2.json())
        if not sent_df.empty:
            st.dataframe(sent_df, use_container_width=True, hide_index=True)
        else:
            st.info("Sin registros.")
    except Exception as e:
        st.error(f"Error al llamar /sentiments: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Heart Dashboard
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.title("🫀 Heart Failure Prediction Dashboard")

    try:
        h_df = load_heart()
    except Exception as e:
        st.error(f"No se pudo conectar a MongoDB (heart_db): {e}")
        st.stop()

    if h_df.empty:
        st.warning("No hay datos en MongoDB (heart_db). Ejecuta el producer y el consumidor Spark primero.")
        st.stop()

    total_h = len(h_df)
    h_df["prediction"] = h_df["prediction"].astype(int)
    risk_map = {0: "Sano", 1: "Riesgo"}
    h_df["prediction_label"] = h_df["prediction"].map(risk_map)

    h_counts = h_df["prediction_label"].value_counts()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total pacientes", total_h)
    m2.metric("Riesgo alto", h_counts.get("Riesgo", 0), f"{h_counts.get('Riesgo',0)/total_h*100:.1f}%")
    m3.metric("Sano", h_counts.get("Sano", 0), f"{h_counts.get('Sano',0)/total_h*100:.1f}%")

    try:
        client_h = MongoClient(HEART_MONGO_URI)
        h_metrics = client_h.heart_db.metrics.find_one({}, {"_id": 0})
        client_h.close()
        if h_metrics:
            m4.metric("Accuracy (test)", f"{h_metrics.get('accuracy', 0)}%")
        else:
            m4.metric("Accuracy (test)", "N/A")
    except Exception:
        m4.metric("Accuracy (test)", "N/A")

    st.divider()

    hl, hr = st.columns(2)
    with hl:
        st.subheader("Distribución de Riesgo")
        h_pie = h_counts.reset_index()
        h_pie.columns = ["predicción", "cantidad"]
        fig_h1 = px.pie(h_pie, values="cantidad", names="predicción",
                        color="predicción",
                        color_discrete_map={"Riesgo": "#e74c3c", "Sano": "#2ecc71"},
                        hole=0.4)
        fig_h1.update_traces(textposition="inside", textinfo="percent+label")
        fig_h1.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_h1, use_container_width=True)

    with hr:
        st.subheader("Riesgo por Tipo de Dolor Torácico")
        if "ChestPainType" in h_df.columns:
            cp = h_df.groupby(["ChestPainType", "prediction_label"]).size().reset_index(name="n")
            fig_h2 = px.bar(cp, x="ChestPainType", y="n", color="prediction_label",
                           color_discrete_map={"Riesgo": "#e74c3c", "Sano": "#2ecc71"},
                           barmode="group",
                           labels={"ChestPainType": "Tipo dolor", "n": "Cantidad"})
            fig_h2.update_layout(margin=dict(t=10, b=10))
            st.plotly_chart(fig_h2, use_container_width=True)
        else:
            st.info("Columna ChestPainType no disponible")

    st.divider()
    st.subheader("Volumen en el tiempo")
    if "fecha_proceso" in h_df.columns:
        h_tl = h_df.set_index("fecha_proceso").resample("1min")["prediction"].count().reset_index()
        h_tl.columns = ["fecha", "cantidad"]
        fig_h3 = px.line(h_tl, x="fecha", y="cantidad", markers=True)
        fig_h3.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig_h3, use_container_width=True)
    else:
        st.info("Sin datos de fecha_proceso")

    st.divider()
    if st.button("🔄 Refresh datos Heart", key="refresh_heart"):
        st.cache_data.clear()
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Heart Predictions (API)
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.title("🔍 Heart Predictions")

    col_a, col_b = st.columns(2)

    try:
        r_summary = requests.get(f"{HEART_API_URL}/risk-summary", timeout=5)
        r_summary.raise_for_status()
        summary = r_summary.json()
        col_a.metric("Total pacientes", summary.get("total_pacientes", 0))
        col_a.metric("% Riesgo alto", f"{summary.get('porcentaje_riesgo', 0)}%")
        col_b.metric("Riesgo alto", summary.get("riesgo_alto", 0))
        col_b.metric("Accuracy modelo", f"{summary.get('accuracy_modelo', 0)}%")
    except Exception:
        col_a.metric("Total pacientes", "—")
        col_b.metric("Accuracy", "—")

    st.divider()
    st.subheader("Últimos pacientes — /patients")
    try:
        r_patients = requests.get(f"{HEART_API_URL}/patients", timeout=5)
        r_patients.raise_for_status()
        patients = pd.DataFrame(r_patients.json())
        if not patients.empty:
            st.dataframe(patients, use_container_width=True, hide_index=True)
        else:
            st.info("Sin registros de pacientes.")
    except Exception as e:
        st.error(f"Error al llamar /patients: {e}")

    st.divider()
    st.subheader("Predicciones filtradas — /predictions")
    risk_filter = st.selectbox("Filtrar por riesgo", ["Todos", "0 (Sano)", "1 (Riesgo)"],
                                key="risk_filter")
    risk_val = "" if risk_filter == "Todos" else risk_filter[0]
    try:
        url = f"{HEART_API_URL}/predictions" + (f"?risk={risk_val}" if risk_val else "")
        r_pred = requests.get(url, timeout=5)
        r_pred.raise_for_status()
        pred_df = pd.DataFrame(r_pred.json())
        if not pred_df.empty:
            st.dataframe(pred_df, use_container_width=True, hide_index=True)
        else:
            st.info("Sin registros.")
    except Exception as e:
        st.error(f"Error al llamar /predictions: {e}")