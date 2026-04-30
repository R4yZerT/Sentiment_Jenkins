from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import current_timestamp
import sys

# 1. Configuración de la Sesión de Spark con el conector de MongoDB
spark = SparkSession.builder \
    .appName("SentimentAnalysisSabaneta") \
    .config("spark.mongodb.write.connection.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    # 2. Carga de Datos
    path = "/opt/spark/data/dataset_sentimientos_500.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # 3. Limpieza: Aseguramos que existan las columnas necesarias
    df_clean = df.dropna(subset=["texto", "etiqueta"])

    # 4. Pipeline de ML
    tokenizer = Tokenizer(inputCol="texto", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    # Índice para la etiqueta
    label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
    
    # Algoritmo de clasificación
    lr = LogisticRegression(maxIter=10, regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

    # 5. Entrenamiento y Predicción
    print("Entrenando modelo...")
    model = pipeline.fit(df_clean)
    predictions = model.transform(df_clean)

    # 6. Preparación de datos para MongoDB
    # Seleccionamos las columnas que quieres guardar y agregamos una fecha de procesamiento
    final_df = predictions.select("texto", "etiqueta", "prediction") \
                          .withColumn("fecha_proceso", current_timestamp())

    # 7. Guardar en MongoDB
    print("Intentando guardar en MongoDB...")
    # Usamos .format("mongodb") y aseguramos la escritura
    final_df.write.format("mongodb") \
        .mode("append") \
        .option("collection", "results") \
        .save()
    
    print("--- ¡ÉXITO! Datos guardados en MongoDB ---")

except Exception as e:
    print(f"--- ERROR FATAL: {str(e)} ---")
    sys.exit(1)

finally:
    spark.stop()
    