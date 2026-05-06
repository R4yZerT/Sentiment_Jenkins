from flask import Flask, jsonify, request
from pymongo import MongoClient
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
import pandas as pd
import os

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/sentiment_db")
DATA_PATH = os.environ.get("DATA_PATH", "dataset_sentimientos_500.csv")
LABEL_MAP = {0: "positivo", 1: "negativo", 2: "neutral"}

client = MongoClient(MONGO_URI)
collection = client.sentiment_db.results


def train_model():
    df = pd.read_csv(DATA_PATH).dropna()
    model = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=1000, stop_words="english")),
        ("clf", LogisticRegression(max_iter=200, C=1.0))
    ])
    model.fit(df["texto"], df["etiqueta"])
    return model


model = train_model()


@app.route("/sentiments", methods=["GET"])
def get_sentiments():
    sentiment_filter = request.args.get("type")
    query = {}
    if sentiment_filter:
        query = {"etiqueta": sentiment_filter}
    data = list(collection.find(query, {"_id": 0}).limit(50))
    for doc in data:
        doc["prediction"] = LABEL_MAP.get(doc.get("prediction"), doc.get("prediction"))
        if "fecha_proceso" in doc:
            doc["fecha_proceso"] = str(doc["fecha_proceso"])
    return jsonify(data)


@app.route("/stats", methods=["GET"])
def get_stats():
    total = collection.count_documents({})
    dist_pipeline = [
        {"$group": {"_id": "$etiqueta", "total": {"$sum": 1}}}
    ]
    distribucion = {d["_id"]: d["total"] for d in collection.aggregate(dist_pipeline)}

    correct = collection.count_documents({
        "$expr": {
            "$eq": [
                "$etiqueta",
                {"$arrayElemAt": [["positivo", "negativo", "neutral"], "$prediction"]}
            ]
        }
    })
    accuracy = round(correct / total * 100, 2) if total else 0

    return jsonify({
        "total": total,
        "distribucion": distribucion,
        "accuracy": accuracy
    })


@app.route("/predict", methods=["POST"])
def predict():
    body = request.get_json(force=True)
    texto = body.get("texto", "").strip()
    if not texto:
        return jsonify({"error": "El campo 'texto' es requerido"}), 400

    label = model.predict([texto])[0]
    proba = model.predict_proba([texto])[0]
    clases = model.classes_
    confianza = {c: round(float(p), 4) for c, p in zip(clases, proba)}

    return jsonify({
        "texto": texto,
        "prediction": label,
        "confianza": confianza
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
