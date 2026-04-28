from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lower, regexp_replace, current_timestamp, udf
from pyspark.sql.types import ArrayType, DoubleType
import sys

# 1. Iniciar sesión
spark = SparkSession.builder \
    .appName("SentimentStreamProcessor") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/sentiment_db.predictions") \
    .getOrCreate()

# 2. Carga y Normalización (El "Fix" definitivo)
path = "/opt/spark/data/dataset_sentimientos_500.csv"
try:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # Si la columna se llama 'etiqueta', la renombramos a 'sentimiento'
    if "etiqueta" in df.columns:
        df = df.withColumnRenamed("etiqueta", "sentimiento")
    
    # Verificación de seguridad: si después de esto no existe 'sentimiento', el script se detiene con info
    if "sentimiento" not in df.columns:
        print(f"ERROR CRÍTICO: Columnas encontradas: {df.columns}")
        sys.exit(1)

except Exception as e:
    print(f"No se pudo leer el archivo en {path}")
    raise e

# 3. Limpieza
df_clean = df.withColumn("texto_clean", lower(col("texto")))
df_clean = df_clean.withColumn("texto_clean", regexp_replace(col("texto_clean"), "[^a-zA-Z\\s]", ""))
df_clean = df_clean.dropna(subset=["sentimiento"])

# 4. Pipeline
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
indexer = StringIndexer(inputCol="sentimiento", outputCol="label") # Ahora sí existe
nb = NaiveBayes(featuresCol="features", labelCol="label", modelType="multinomial")

# 5. Ejecución
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, nb])
model = pipeline.fit(df_clean)

# 6. Predicciones y MongoDB
vector_to_list = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
predictions = model.transform(df_clean)

final_output = predictions.select(
    col("texto"),
    col("prediction"),
    vector_to_list(col("probability")).alias("confianza"),
    current_timestamp().alias("fecha")
)

final_output.write.format("mongodb").mode("append").save()
print("¡Éxito! Datos guardados en MongoDB.")