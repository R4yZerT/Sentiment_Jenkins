from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
import sys

# 1. Configuración de la Sesión
spark = SparkSession.builder \
    .appName("SentimentAnalysisSabaneta") \
    .config("spark.mongodb.output.uri", "mongodb://sentiment_mongo:27017/sentiment_db.results") \
    .getOrCreate()

try:
    # 2. Carga de Datos - Usando la ruta correcta del contenedor
    path = "/opt/spark/data/dataset_sentimientos_500.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # 3. Limpieza: Usamos 'etiqueta' que es el nombre real detectado
    df_clean = df.dropna(subset=["texto", "etiqueta"])

    # 4. Pipeline de ML
    tokenizer = Tokenizer(inputCol="texto", outputCol="words")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    hashingTF = HashingTF(inputCol="filtered", outputCol="rawFeatures")
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    
    # CORRECCIÓN AQUÍ: Cambiado de 'sentimiento' a 'etiqueta'
    label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
    
    lr = LogisticRegression(maxIter=10)

    pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

    # 5. Entrenamiento
    print("Entrenando modelo con columna 'etiqueta'...")
    model = pipeline.fit(df_clean)
    print("--- PROCESO COMPLETADO CON ÉXITO ---")

except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)