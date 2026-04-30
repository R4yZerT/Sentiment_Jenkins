from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
import sys

# 1. Configuración de la Sesión de Spark
# Incluimos la configuración de MongoDB aquí por si acaso
spark = SparkSession.builder \
    .appName("SentimentAnalysisSabaneta") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/sentiment_db.results") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("--- INICIANDO PROCESAMIENTO ---")

try:
    # 2. Carga de Datos
    path = "/opt/spark/data/dataset_sentimientos_500.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    print(f"Columnas finales para el modelo: {df.columns}")
    
    # Limpieza básica
    df_clean = df.dropna(subset=["texto", "etiqueta"])

    # 4. Pipeline de Machine Learning
    # Ahora 'inputCol' coincide perfectamente con el nombre en el DataFrame
    tokenizer = Tokenizer(inputCol="texto", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    # Convertimos la categoría de texto a índice numérico
    label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
    
    lr = LogisticRegression(maxIter=10, regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

    # 5. Entrenamiento
    print("Entrenando modelo...")
    model = pipeline.fit(df_clean)
    predictions = model.transform(df_clean)

    # 6. Guardar en MongoDB
    print("Guardando resultados en MongoDB...")
    # Seleccionamos solo lo relevante para no saturar la base de datos
    final_df = predictions.select("texto", "etiqueta", "prediction")
    
    final_df.write.format("mongodb").mode("append").save()
    
    print("--- PROCESO COMPLETADO CON ÉXITO ---")

except Exception as e:
    print(f"ERROR FATAL: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()