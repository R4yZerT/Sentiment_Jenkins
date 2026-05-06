import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    data_path = "/opt/spark/data/dataset_sentimientos_500.csv"

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
        df = spark.read.option("header", "true").schema(schema).csv(data_path).dropna()

        # Split 80/20
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        print(f"DEBUG: Train={train_df.count()} | Test={test_df.count()}")

        # Pipeline de ML
        tokenizer      = Tokenizer(inputCol="texto", outputCol="words")
        remover        = StopWordsRemover(inputCol="words", outputCol="filtered")
        hashingTF      = HashingTF(inputCol="filtered", outputCol="rawFeatures", numFeatures=1000)
        idf            = IDF(inputCol="rawFeatures", outputCol="features")
        label_stringIdx = StringIndexer(inputCol="etiqueta", outputCol="label")
        lr             = LogisticRegression(maxIter=10, regParam=0.01)

        pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, label_stringIdx, lr])

        # Entrenar solo con train
        model = pipeline.fit(train_df)

        # Evaluar sobre test
        test_predictions = model.transform(test_df)

        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(test_predictions)
        print(f"--- ACCURACY en test set: {accuracy:.4f} ({accuracy*100:.2f}%) ---")

        # Predecir sobre todo el dataset para guardar en MongoDB
        all_predictions = model.transform(df)
        count = all_predictions.count()
        print(f"DEBUG: Filas procesadas: {count}")

        if count > 0:
            all_predictions.select("texto", "etiqueta", "prediction") \
                .withColumn("fecha_proceso", current_timestamp()) \
                .withColumn("accuracy_test", lit(round(accuracy * 100, 2))) \
                .withColumn("split", lit("full")) \
                .write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("database", "sentiment_db") \
                .option("collection", "results") \
                .option("writeConcern.w", "1") \
                .save()

            # Guardar métricas en colección aparte
            metrics_data = [(round(accuracy * 100, 2), train_df.count(), test_df.count())]
            metrics_df = spark.createDataFrame(metrics_data, ["accuracy", "train_size", "test_size"]) \
                .withColumn("fecha", current_timestamp())
            metrics_df.write \
                .format("mongodb") \
                .mode("overwrite") \
                .option("database", "sentiment_db") \
                .option("collection", "metrics") \
                .option("writeConcern.w", "1") \
                .save()

            print("--- ÉXITO: Datos y métricas guardados en MongoDB ---")
        else:
            print("ERROR: El DataFrame está vacío.")
            sys.exit(1)

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
