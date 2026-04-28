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
df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# --- EL ARREGLO ESTÁ AQUÍ ---
# 1. Creamos la columna 'texto_clean' (minúsculas y sin caracteres raros)
df_clean = df.withColumn("texto_clean", regexp_replace(lower(col("texto")), "[^a-zA-Z\\s]", ""))

# 2. Renombramos de forma SEGURA y forzada. 
# Si el CSV traía 'etiqueta', lo forzamos a llamarse 'sentimiento' en df_clean
if "etiqueta" in df_clean.columns:
    df_clean = df_clean.withColumnRenamed("etiqueta", "sentimiento")

# 3. Borramos nulos basados en la NUEVA columna
df_clean = df_clean.dropna(subset=["sentimiento", "texto_clean"])

print("--- ESQUEMA JUSTO ANTES DEL PIPELINE ---")
df_clean.printSchema() # Aquí DEBE decir 'sentimiento'

# 4. Pipeline NLP (Asegúrate de que inputCol sea "sentimiento")
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# El Indexer ahora sí encontrará la columna
indexer = StringIndexer(inputCol="sentimiento", outputCol="label") 
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