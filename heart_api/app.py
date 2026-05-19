from flask import Flask, jsonify, request
from pymongo import MongoClient
import os

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/heart_db")

client = MongoClient(MONGO_URI)
db = client.heart_db
predictions = db.predictions
metrics = db.metrics


@app.route("/patients", methods=["GET"])
def get_patients():
    limit = request.args.get("limit", 50, type=int)
    data = list(predictions.find({}, {"_id": 0}).sort("fecha_proceso", -1).limit(limit))
    for doc in data:
        if "fecha_proceso" in doc:
            doc["fecha_proceso"] = str(doc["fecha_proceso"])
    return jsonify(data)


@app.route("/predictions", methods=["GET"])
def get_predictions():
    risk = request.args.get("risk")
    query = {}
    if risk is not None:
        try:
            query["prediction"] = int(risk)
        except ValueError:
            query["prediction_label"] = risk
    data = list(predictions.find(query, {"_id": 0}).sort("fecha_proceso", -1).limit(100))
    for doc in data:
        if "fecha_proceso" in doc:
            doc["fecha_proceso"] = str(doc["fecha_proceso"])
    return jsonify(data)


@app.route("/stats", methods=["GET"])
def get_stats():
    total = predictions.count_documents({})
    dist_pipeline = [{"$group": {"_id": "$prediction_label", "total": {"$sum": 1}}}]
    distribucion = {d["_id"]: d["total"] for d in predictions.aggregate(dist_pipeline)}

    correct = predictions.count_documents(
        {"$expr": {"$eq": ["$heart_disease_label", "$prediction"]}}
    )
    accuracy = round(correct / total * 100, 2) if total else 0

    return jsonify(
        {"total": total, "distribucion": distribucion, "accuracy": accuracy}
    )


@app.route("/risk-summary", methods=["GET"])
def get_risk_summary():
    total = predictions.count_documents({})
    high_risk = predictions.count_documents({"prediction": 1})
    low_risk = predictions.count_documents({"prediction": 0})

    latest_metrics = list(metrics.find({}, {"_id": 0}).sort("fecha", -1).limit(1))
    model_accuracy = latest_metrics[0].get("accuracy", 0) if latest_metrics else 0

    return jsonify(
        {
            "total_pacientes": total,
            "riesgo_alto": high_risk,
            "riesgo_bajo": low_risk,
            "porcentaje_riesgo": round(high_risk / total * 100, 2) if total else 0,
            "accuracy_modelo": model_accuracy,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)