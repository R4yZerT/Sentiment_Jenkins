from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp
import shutil
import os
import sys

def main():
    # 1. Configuración de rutas
    data_path = "/opt/spark/data/"
    checkpoint_path = "/tmp/checkpoint"
    
    # Limpiar estado previo
    if os.path.exists(checkpoint_path):
        shutil.rmtree(checkpoint_path)

    # 2. Inicialización
    spark = SparkSession.builder \
        .appName("SentimentStreamingSabaneta") \
        .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    # 3. Entrenamiento con datos históricos (debe existir un archivo inicial)
    # Si no hay archivos, el script espera; si hay, entrena.
    try:
        df_batch = spark.read.option("header", "true").schema(schema).csv(data_path)
        
        # Pipeline de ML
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])
        model = pipeline.fit(df_batch.dropna())
        print("--- MODELO ENTRENADO CON ÉXITO ---")

        # 4. STREAMING
        # IMPORTANTE: .option("maxFilesPerTrigger", 1) evita que el stream colapse
        df_stream = spark.readStream \
            .option("header", "true") \
            .schema(schema) \
            .csv(data_path)
        
        predictions = model.transform(df_stream)

        # 5. Escritura a MongoDB
        query = predictions.select("texto", "etiqueta", "prediction") \
            .withColumn("fecha_proceso", current_timestamp()) \
            .writeStream \
            .format("mongodb") \
            .option("checkpointLocation", checkpoint_path) \
            .option("database", "sentiment_db") \
            .option("collection", "results") \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        print("--- ESPERANDO DATOS EN TIEMPO REAL... ---")
        query.awaitTermination()

    except Exception as e:
        print(f"--- ERROR: {str(e)} ---")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()