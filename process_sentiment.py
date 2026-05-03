import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp

def main():
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"
    
    spark = SparkSession.builder \
        .appName("SentimentBatchSabaneta") \
        .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    try:
        print("Leyendo datos y entrenando modelo...")
        df = spark.read.option("header", "true").schema(schema).csv(data_path)
        
        # Pipeline de ML
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])
        model = pipeline.fit(df.dropna())
        
        print("Modelo listo. Procesando predicciones...")
        predictions = model.transform(df)

        # Guardar en MongoDB (Batch)
        predictions.select("texto", "etiqueta", "prediction") \
            .withColumn("fecha_proceso", current_timestamp()) \
            .write \
            .format("mongodb") \
            .mode("append") \
            .save()
        
        print("Proceso finalizado con éxito. Datos guardados en MongoDB.")

    except Exception as e:
        print(f"--- ERROR DURANTE LA EJECUCIÓN ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()