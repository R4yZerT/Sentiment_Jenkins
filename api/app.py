from flask import Flask, jsonify, request
from pymongo import MongoClient
import os

app = Flask(__name__)

# Configuración de MongoDB
mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/sentiment_db")
client = MongoClient(mongo_uri)
db = client.sentiment_db
collection = db.predictions

@app.route('/sentiments', methods=['GET'])
def get_sentiments():
    """Retorna listado con filtros opcionales por sentimiento """
    sentiment_filter = request.args.get('type') # Ejemplo: /sentiments?type=positivo
    query = {}
    if sentiment_filter:
        query = {"prediction": sentiment_filter}
    
    # Obtenemos los últimos 50 registros para no saturar la respuesta
    data = list(collection.find(query, {'_id': 0}).sort("fecha", -1).limit(50))
    return jsonify(data)

@app.route('/stats', methods=['GET'])
def get_stats():
    """Retorna la distribución de clases y métricas """
    pipeline = [
        {"$group": {"_id": "$prediction", "total": {"$sum": 1}}}
    ]
    stats = list(collection.aggregate(pipeline))
    return jsonify(stats)

@app.route('/predict', methods=['POST'])
def predict_manual():
    """Inferencia manual sobre texto nuevo """
    # En un entorno real, aquí cargarías el modelo de Spark guardado
    # Por ahora, simulamos la estructura de respuesta
    content = request.json
    text = content.get("texto", "")
    return jsonify({
        "texto": text,
        "prediction": "neutral", # Placeholder para el pipeline de inferencia
        "status": "endpoint_active"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)