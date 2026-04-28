from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lower, regexp_replace, current_timestamp, udf
from pyspark.sql.types import ArrayType, DoubleType

# 1. Iniciar sesión
spark = SparkSession.builder \
    .appName("SentimentStreamProcessor") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/sentiment_db.predictions") \
    .getOrCreate()

# 2. Carga del Dataset
# IMPORTANTE: Esta ruta requiere que el archivo esté mapeado en el contenedor
path = "/opt/spark/data/dataset_sentimientos_500.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

# NORMALIZACIÓN DE COLUMNAS:
# Si el archivo viene con 'etiqueta', lo renombramos a 'sentimiento' para que el indexer lo encuentre
if "etiqueta" in df.columns:
    df = df.withColumnRenamed("etiqueta", "sentimiento")

print("Columnas detectadas en el DataFrame final:")
df.printSchema()

# 3. Limpieza de Texto
df_clean = df.withColumn("texto_clean", lower(col("texto")))
df_clean = df_clean.withColumn("texto_clean", regexp_replace(col("texto_clean"), "[^a-zA-Z\\s]", ""))
df_clean = df_clean.dropna(subset=["sentimiento"]) # Evita errores en el fit

# 4. Pipeline NLP
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Aquí usamos 'sentimiento' porque ya lo normalizamos en el paso 2
indexer = StringIndexer(inputCol="sentimiento", outputCol="label")
nb = NaiveBayes(featuresCol="features", labelCol="label", modelType="multinomial")

# 5. Entrenar
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, nb])
model = pipeline.fit(df_clean)

# 6. Predicciones y Guardado
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