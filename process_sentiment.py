import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression

def main():
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"
    
    # Configuración de sesión con parámetros explícitos para el conector
    # Esto elimina la ambigüedad que causa que no se creen las colecciones
    spark = SparkSession.builder \
        .appName("SentimentBatchSabaneta") \
        .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("texto", StringType(), True),
        StructField("etiqueta", StringType(), True)
    ])

    try:
        df = spark.read.option("header", "true").schema(schema).csv(data_path)
        
        # Pipeline de ML (Procesamiento)
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
        print(f"DEBUG: Filas listas para persistir: {count}")

        if count > 0:
            # Escritura con WriteConcern explícito para asegurar la confirmación del nodo
            predictions.select("texto", "etiqueta", "prediction") \
                .withColumn("fecha_proceso", current_timestamp()) \
                .write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("database", "sentiment_db") \
                .option("collection", "results") \
                .option("writeConcern.w", "1") \
                .save()
            print("--- ÉXITO: Datos confirmados en MongoDB ---")
        else:
            print("ERROR: DataFrame vacío.")
            sys.exit(1)

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()