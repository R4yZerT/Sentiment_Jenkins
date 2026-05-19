from flask import Flask, jsonify, request
from pymongo import MongoClient
import os

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/sentiment_db")

client = MongoClient(MONGO_URI)
collection = client.sentiment_db.results


@app.route("/sentiments", methods=["GET"])
def get_sentiments():
    sentiment_filter = request.args.get("type")
    query = {}
    if sentiment_filter:
        query = {"etiqueta": sentiment_filter}
    data = list(collection.find(query, {"_id": 0}).limit(50))
    for doc in data:
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
        "$expr": {"$eq": ["$etiqueta", "$prediction"]}
    })
    accuracy = round(correct / total * 100, 2) if total else 0

    return jsonify({
        "total": total,
        "distribucion": distribucion,
        "accuracy": accuracy
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
