from flask import Flask, jsonify
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient("mongodb://sentiment_mongo:27017/")
db = client.sentiment_db

@app.route('/resultados', methods=['GET'])
def get_resultados():
    # Extrae todo de la colección results
    data = list(db.results.find({}, {'_id': 0}))
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)