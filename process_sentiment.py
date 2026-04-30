from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp
import sys

def main():
    # 1. Configuración de la Sesión
    spark = SparkSession.builder \
        .appName("SentimentStreamingSabaneta") \
        .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Definición de esquema para el CSV
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    try:
        path = "/opt/spark/data/" 
        
        # 2. Pipeline de ML (Estructura)
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

        # 3. Entrenamiento (Batch - Necesario para que el modelo aprenda)
        print("Entrenando modelo con datos históricos...")
        df_batch = spark.read.option("header", "true").schema(schema).csv(path)
        model = pipeline.fit(df_batch.dropna())
        print("Modelo entrenado con éxito.")

        # 4. Lectura de Streaming (Inferencia en tiempo real)
        print("Iniciando flujo de streaming...")
        df_stream = spark.readStream.option("header", "true").schema(schema).csv(path)
        
        # 5. Transformación del flujo
        predictions = model.transform(df_stream)

        # 6. Preparación y escritura en MongoDB
        final_df = predictions.select("texto", "etiqueta", "prediction") \
                              .withColumn("fecha_proceso", current_timestamp())

        query = final_df.writeStream \
            .format("mongodb") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .option("database", "sentiment_db") \
            .option("collection", "results") \
            .outputMode("append") \
            .start()
        
        query.awaitTermination()

    except Exception as e:
        print(f"--- ERROR FATAL: {str(e)} ---")
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()