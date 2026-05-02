import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp

def main():
    data_path = "/opt/spark/data/"
    # Checkpoint dinámico para evitar colisiones entre ejecuciones
    checkpoint_path = f"/tmp/checkpoint_{int(time.time())}"

    spark = SparkSession.builder \
        .appName("SentimentStreamingSabaneta") \
        .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    try:
        print("Entrenando modelo con datos iniciales...")
        # Leemos los archivos actuales para entrenar el modelo batch
        df_batch = spark.read.option("header", "true").schema(schema).csv(data_path)
        
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])
        model = pipeline.fit(df_batch.dropna())
        print("Modelo listo.")

        # CONFIGURACIÓN DE STREAMING ROBUSTA
        # 4. STREAMING
        df_stream = spark.readStream \
            .option("header", "true") \
            .option("ignoreCorruptFiles", "true") \
            .option("ignoreChanges", "true") \
            .option("latestFirst", "false") \
            .option("maxFilesPerTrigger", 1) \
            .schema(schema) \
            .csv(data_path)
        
        predictions = model.transform(df_stream)

        print(f"Iniciando flujo. Checkpoint en: {checkpoint_path}")

        query = predictions.select("texto", "etiqueta", "prediction") \
            .withColumn("fecha_proceso", current_timestamp()) \
            .writeStream \
            .format("mongodb") \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        query.awaitTermination()

    except Exception as e:
        print(f"--- ERROR DURANTE LA EJECUCIÓN ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()