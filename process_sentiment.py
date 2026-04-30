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

    try:
        # 2. Definición del Esquema (Dentro de la función para evitar errores de scope)
        schema = StructType([
            StructField("texto", StringType(), True),
            StructField("etiqueta", StringType(), True)
        ])

        # 3. Lectura como Stream
        # Asegúrate de que esta carpeta contenga tus archivos .csv
        path = "/opt/spark/data/" 
        print(f"Leyendo datos desde: {path}")
        df = spark.readStream.option("header", "true").schema(schema).csv(path)
        
        # 4. Pipeline de ML
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

        # 5. Entrenamiento (Modelo inicial)
        # Nota: En streaming real, cargarías un modelo previamente guardado (.load)
        # Aquí usamos un modelo base para transformar el flujo
        model = pipeline.fit(df.dropna()) 
        predictions = model.transform(df)

        # 6. Preparación de datos
        final_df = predictions.select("texto", "etiqueta", "prediction") \
                              .withColumn("fecha_proceso", current_timestamp())

        # 7. Escritura en MongoDB (Streaming)
        print("Iniciando escritura en MongoDB...")
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