import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression

def main():
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"
    
    # 1. Configuración de sesión usando el nombre del servicio en Docker
    spark = SparkSession.builder \
        .appName("SentimentBatchSabaneta") \
        .config("spark.mongodb.output.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    try:
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
        predictions = model.transform(df)

        count = predictions.count()
        print(f"DEBUG: Filas procesadas: {count}")

        if count > 0:
            # 2. Escribir usando el método directo de MongoSpark
            predictions.select("texto", "etiqueta", "prediction") \
                .withColumn("fecha_proceso", current_timestamp()) \
                .write \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .mode("overwrite") \
                .option("database", "sentiment_db") \
                .option("collection", "results") \
                .save()
            print("--- ÉXITO: Datos guardados en MongoDB ---")
        else:
            print("ERROR: El DataFrame está vacío.")
            sys.exit(1)

            
    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()