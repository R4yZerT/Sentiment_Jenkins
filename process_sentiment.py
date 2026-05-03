import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression

def main():
    # Usamos la ruta donde el Jenkinsfile coloca el archivo
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"
    
    # IMPORTANTE: Definimos todo en la sesión para evitar ambigüedades en el conector
    # La URI incluye la base de datos y la colección por defecto.
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
        print(f"DEBUG: Filas procesadas exitosamente: {count}")

        if count > 0:
            # Escribimos directo sin pasar opciones adicionales que puedan confundir al conector
            predictions.select("texto", "etiqueta", "prediction") \
                .withColumn("fecha_proceso", current_timestamp()) \
                .write \
                .format("mongodb") \
                .mode("overwrite") \
                .save()
            print("--- ÉXITO: Datos persistidos en sentiment_db.results ---")
        else:
            print("ERROR: El DataFrame resultante está vacío.")
            sys.exit(1)

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()