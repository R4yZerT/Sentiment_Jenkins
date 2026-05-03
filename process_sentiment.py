import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp

def main():
    # Asegúrate de que esta ruta sea accesible dentro del contenedor spark_master
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"
    
    print("--- INICIANDO PROCESO SPARK ---")
    
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
        print(f"Leyendo archivo en: {data_path}")
        df = spark.read.option("header", "true").schema(schema).csv(data_path)
        
        # Verificar si leyó algo
        if df.rdd.isEmpty():
            print("ERROR: El archivo CSV está vacío o no se encontró.")
            sys.exit(1)
            
        print(f"Total filas leídas: {df.count()}")
        
        # Pipeline de ML
        tokenizer = Tokenizer(inputCol="texto", outputCol="words")
        remover = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])
        
        print("Entrenando modelo...")
        model = pipeline.fit(df.dropna())
        
        print("Realizando predicciones...")
        predictions = model.transform(df)

        # DEBUG: Contar resultados antes de guardar
        count = predictions.count()
        print(f"Total filas procesadas para guardar: {count}")

        if count > 0:
            print("Guardando en MongoDB...")
            # Guardar en MongoDB usando configuraciones explícitas
            predictions.select("texto", "etiqueta", "prediction") \
                .withColumn("fecha_proceso", current_timestamp()) \
                .write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("spark.mongodb.write.database", "sentiment_db") \
                .option("spark.mongodb.write.collection", "results") \
                .save()
            print("--- Guardado ejecutado con éxito ---")
        else:
            print("ERROR: No hay datos para guardar.")
            sys.exit(1)

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
        print("Sesión Spark cerrada.")

if __name__ == "__main__":
    main()