from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lower, regexp_replace, current_timestamp, udf
from pyspark.sql.types import ArrayType, DoubleType

# 1. Iniciar sesión
# Usamos el conector de MongoDB para Spark
spark = SparkSession.builder \
    .appName("SentimentStreamProcessor") \
    .config("spark.mongodb.write.connection.uri", "mongodb://mongodb:27017/sentiment_db.predictions") \
    .getOrCreate()

# 2. Carga del Dataset
# NOTA: Esta ruta debe estar mapeada como volumen en Docker
try:
    path = "/opt/spark/data/dataset_sentimientos_500.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # --- PASO CRÍTICO: RENOMBRAR COLUMNA ---
    # Si el CSV trae "etiqueta", lo cambiamos a "sentimiento" para evitar el error Py4JJavaError
    if "etiqueta" in df.columns:
        df = df.withColumnRenamed("etiqueta", "sentimiento")
    
    print("Esquema detectado y normalizado:")
    df.printSchema()
except Exception as e:
    print(f"Error al leer el archivo en {path}. Verifica los volúmenes de Docker.")
    raise e

# 3. Limpieza de Texto (Pre-procesamiento)
# Creamos una columna limpia sin caracteres especiales y en minúsculas
df_clean = df.withColumn("texto_clean", lower(col("texto")))
df_clean = df_clean.withColumn("texto_clean", regexp_replace(col("texto_clean"), "[^a-zA-Z\\s]", ""))
# Eliminamos filas donde el texto o el sentimiento sean nulos
df_clean = df_clean.dropna(subset=["texto_clean", "sentimiento"])

# 4. Definición de las Etapas del Pipeline NLP
tokenizer = Tokenizer(inputCol="texto_clean", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
idf = IDF(inputCol="rawFeatures", outputCol="features")

# Aquí usamos "sentimiento" porque ya lo normalizamos arriba
indexer = StringIndexer(inputCol="sentimiento", outputCol="label")
nb = NaiveBayes(featuresCol="features", labelCol="label", modelType="multinomial")

# 5. Construir y Entrenar el Pipeline
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, indexer, nb])
print("Entrenando el modelo...")
model = pipeline.fit(df_clean)

# 6. Realizar Predicciones
predictions = model.transform(df_clean)

# UDF para convertir el vector de probabilidad a una lista (para MongoDB)
vector_to_list = udf(lambda v: v.toArray().tolist(), ArrayType(DoubleType()))

# 7. Estructurar salida final
final_output = predictions.select(
    col("texto"),
    col("prediction"),
    vector_to_list(col("probability")).alias("confianza"),
    current_timestamp().alias("fecha")
)

# 8. Guardar en MongoDB
print("Guardando resultados en MongoDB...")
final_output.write.format("mongodb").mode("append").save()

print("Procesamiento completado con éxito.")