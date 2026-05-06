import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
import requests
import os

st.set_page_config(page_title="Sentiment Dashboard", layout="wide", page_icon="🎭")

LABEL_MAP = {0: "positivo", 1: "negativo", 2: "neutral"}
COLOR_MAP = {"positivo": "#2ecc71", "negativo": "#e74c3c", "neutral": "#3498db"}
MONGO_URI   = os.environ.get("MONGO_URI",    "mongodb://localhost:27017/sentiment_db")
API_URL     = os.environ.get("API_URL",      "http://localhost:5001")
ENRICHED    = os.environ.get("ENRICHED_CSV", "dataset_final.csv")


@st.cache_data(ttl=30)
def load_mongo():
    client = MongoClient(MONGO_URI)
    docs = list(client.sentiment_db.results.find({}, {"_id": 0}))
    client.close()
    df = pd.DataFrame(docs)
    if "prediction" in df.columns:
        df["prediction"] = df["prediction"].map(LABEL_MAP).fillna(df["prediction"].astype(str))
    else:
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


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "📈 Stats API", "🔮 Predictor"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Dashboard (datos de MongoDB)
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.title("🎭 Sentiment Analysis Dashboard")

    try:
        df = load_mongo()
    except Exception as e:
        st.error(f"No se pudo conectar a MongoDB: {e}")
        st.stop()

    if df.empty:
        st.warning("No hay datos en MongoDB.")
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
                     color="sentimiento", color_discrete_map=COLOR_MAP, hole=0.4)
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with cr:
        st.subheader("Etiqueta real vs Predicción")
        cmp = df.groupby(["etiqueta", "prediction"]).size().reset_index(name="n")
        fig2 = px.bar(cmp, x="etiqueta", y="n", color="prediction",
                      color_discrete_map=COLOR_MAP, barmode="group",
                      labels={"etiqueta": "Etiqueta real", "n": "Cantidad", "prediction": "Predicción"})
        fig2.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig2, use_container_width=True)

    # Accuracy real del test set (guardada por Spark en colección metrics)
    try:
        client_m = MongoClient(MONGO_URI)
        metrics = client_m.sentiment_db.metrics.find_one({}, {"_id": 0})
        client_m.close()
        if metrics:
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Accuracy (test set 20%)", f"{metrics.get('accuracy', 0)}%")
            col_b.metric("Train size", metrics.get("train_size", "-"))
            col_c.metric("Test size",  metrics.get("test_size",  "-"))
        else:
            correct = (df["etiqueta"] == df["prediction"]).sum()
            st.metric("Accuracy del modelo", f"{correct/total*100:.1f}%",
                      help="Corre el pipeline de Jenkins para ver el accuracy real del test set")
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
# TAB 2 — Stats de la API (/stats y /sentiments)
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.title("📈 Stats de la API")

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
                      color="sentimiento", color_discrete_map=COLOR_MAP,
                      text="cantidad")
        fig4.update_traces(textposition="outside")
        fig4.update_layout(showlegend=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig4, use_container_width=True)

    except Exception as e:
        st.error(f"No se pudo conectar a la API ({API_URL}/stats): {e}")

    st.divider()
    st.subheader("Últimos registros — /sentiments")
    tipo = st.selectbox("Filtrar por tipo", ["", "positivo", "negativo", "neutral"],
                        format_func=lambda x: "Todos" if x == "" else x)
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
# TAB 3 — Predictor en tiempo real (/predict)
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.title("🔮 Predictor de sentimiento")
    st.caption("Escribe un texto y el modelo te dice si es positivo, negativo o neutral.")

    texto = st.text_area("Texto a analizar", height=120,
                         placeholder="Ej: This product is absolutely amazing!")

    if st.button("Predecir", type="primary"):
        if not texto.strip():
            st.warning("Escribe algo primero.")
        else:
            try:
                r = requests.post(f"{API_URL}/predict",
                                  json={"texto": texto}, timeout=10)
                r.raise_for_status()
                result = r.json()

                label = result.get("prediction", "")
                emoji = {"positivo": "😊", "negativo": "😠", "neutral": "😐"}.get(label, "❓")
                color = COLOR_MAP.get(label, "#888")

                st.markdown(f"### {emoji} Resultado: **:{color.replace('#','')}[{label.upper()}]**")
                st.markdown(f"**Sentimiento detectado:** `{label}`")

                confianza = result.get("confianza", {})
                if confianza:
                    st.subheader("Confianza por clase")
                    conf_df = pd.DataFrame(
                        list(confianza.items()), columns=["sentimiento", "probabilidad"]
                    ).sort_values("probabilidad", ascending=False)
                    fig5 = px.bar(conf_df, x="sentimiento", y="probabilidad",
                                  color="sentimiento", color_discrete_map=COLOR_MAP,
                                  range_y=[0, 1], text=conf_df["probabilidad"].apply(lambda x: f"{x:.1%}"))
                    fig5.update_traces(textposition="outside")
                    fig5.update_layout(showlegend=False, margin=dict(t=10, b=10))
                    st.plotly_chart(fig5, use_container_width=True)

            except Exception as e:
                st.error(f"Error al llamar /predict: {e}")
