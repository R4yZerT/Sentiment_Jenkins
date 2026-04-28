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
# Asegúrate de que el nombre coincida exactamente con tu archivo en la raíz
# Busca la línea donde cargas el csv y cámbiala por esto:
df = spark.read.option("header", "true").option("inferSchema", "true").csv("/opt/spark/data/dataset_sentimientos_500.csv")# 3. Limpieza de Texto (Pre-procesamiento)
print("Columnas detectadas en el CSV:")
df.printSchema()
# Convertimos a minúsculas y eliminamos caracteres especiales/puntuación
df_clean = df.withColumn("texto_clean", lower(col("texto")))
df_clean = df_clean.withColumn("texto_clean", regexp_replace(col("texto_clean"), "[^a-zA-Z\\s]", ""))

# 4. Definición de las Etapas del Pipeline NLP
# Dividir el texto en palabras
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")

# Eliminar palabras que no aportan significado (Stopwords)
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

# Convertir palabras a frecuencias numéricas (HashingTF)
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)

# Escalar las frecuencias (IDF) para resaltar palabras importantes
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Convertir la etiqueta 'positivo/negativo' a números (0, 1, 2)
indexer = StringIndexer(inputCol="etiqueta", outputCol="label")
# 5. El Modelo: Naive Bayes
nb = NaiveBayes(featuresCol="features", labelCol="label", modelType="multinomial")

# 6. Construir y Entrenar el Pipeline
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, nb])
model = pipeline.fit(df_clean)

# UDF para convertir DenseVector a lista (MongoDB no puede serializar VectorUDT)
vector_to_list = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))

# 7. Realizar Predicciones
predictions = model.transform(df_clean)

# Seleccionamos solo lo que pide el PDF: texto, predicción y confianza
final_output = predictions.select(
    col("texto"),
    col("prediction"),
    vector_to_list(col("probability")).alias("confianza"),
    current_timestamp().alias("fecha")
)

# 8. Guardar en MongoDB
final_output.write.format("mongodb").mode("append").save()

print("Procesamiento completado y datos guardados en MongoDB.")