from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lower, regexp_replace, current_timestamp, udf
from pyspark.sql.types import ArrayType, DoubleType
import sys

# Iniciar sesión
spark = SparkSession.builder \
    .appName("SentimentStreamProcessor") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/sentiment_db.predictions") \
    .getOrCreate()

# Carga del Dataset
path = "/opt/spark/data/dataset_sentimientos_500.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# --- NORMALIZACIÓN RADICAL ---
# Buscamos 'etiqueta' o 'sentimiento' y lo estandarizamos a 'target'
current_cols = df_raw.columns
target_col = ""

if "sentimiento" in current_cols:
    target_col = "sentimiento"
elif "etiqueta" in current_cols:
    df_raw = df_raw.withColumnRenamed("etiqueta", "sentimiento")
    target_col = "sentimiento"
else:
    print(f"ERROR: No se encontró columna de etiqueta. Columnas: {current_cols}")
    sys.exit(1)

# Limpieza y preparación
df_clean = df_raw.withColumn("texto_clean", regexp_replace(lower(col("texto")), "[^a-zA-Z\\s]", ""))
df_clean = df_clean.dropna(subset=["sentimiento", "texto_clean"])

# Pipeline con nombres estables
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")
indexer = StringIndexer(inputCol="sentimiento", outputCol="label") 
nb = NaiveBayes(featuresCol="features", labelCol="label", modelType="multinomial")

pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, nb])
model = pipeline.fit(df_clean)

# Predicciones y Guardado
vector_to_list = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))
predictions = model.transform(df_clean)

final_output = predictions.select(
    col("texto"),
    col("prediction"),
    vector_to_list(col("probability")).alias("confianza"),
    current_timestamp().alias("fecha")
)

final_output.write.format("mongodb").mode("append").save()
print("Procesamiento completado con éxito.")