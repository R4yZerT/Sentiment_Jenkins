import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    IndexToString,
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def main():
    data_path = os.environ.get("DATA_PATH", "/opt/spark/data/heart.csv")
    model_path = os.environ.get("MODEL_PATH", "/opt/spark/models/heart_model")
    mongo_uri = os.environ.get(
        "MONGO_URI", "mongodb://sentiment_mongo:27017/heart_db.metrics"
    )

    spark = (
        SparkSession.builder.appName("HeartModelTraining")
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType(
        [
            StructField("Age", IntegerType(), True),
            StructField("Sex", StringType(), True),
            StructField("ChestPainType", StringType(), True),
            StructField("RestingBP", IntegerType(), True),
            StructField("Cholesterol", IntegerType(), True),
            StructField("FastingBS", IntegerType(), True),
            StructField("RestingECG", StringType(), True),
            StructField("MaxHR", IntegerType(), True),
            StructField("ExerciseAngina", StringType(), True),
            StructField("Oldpeak", DoubleType(), True),
            StructField("ST_Slope", StringType(), True),
            StructField("HeartDisease", IntegerType(), True),
        ]
    )

    try:
        df = spark.read.option("header", "true").schema(schema).csv(data_path).dropna()
        print(f"DEBUG: Total registros = {df.count()}")

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        print(f"DEBUG: Train={train_df.count()} | Test={test_df.count()}")

        cat_cols = ["Sex", "ChestPainType", "RestingECG", "ExerciseAngina", "ST_Slope"]
        num_cols = ["Age", "RestingBP", "Cholesterol", "FastingBS", "MaxHR", "Oldpeak"]

        indexers = [
            StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in cat_cols
        ]

        encoders = [
            OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_vec")
            for c in cat_cols
        ]

        assembler = VectorAssembler(
            inputCols=num_cols + [f"{c}_vec" for c in cat_cols],
            outputCol="features",
        )

        label_indexer = StringIndexer(
            inputCol="HeartDisease", outputCol="label", handleInvalid="keep"
        )

        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=100,
            maxDepth=10,
            seed=42,
        )

        pipeline = Pipeline(
            stages=indexers + encoders + [assembler, label_indexer, rf]
        )

        model = pipeline.fit(train_df)

        # Obtener labels del StringIndexer fitteado
        fitted_label_indexer = model.stages[len(indexers) + len(encoders) + 1]
        heart_labels = fitted_label_indexer.labelsArray[0]
        converter = IndexToString(
            inputCol="prediction",
            outputCol="prediction_label",
            labels=heart_labels,
        )

        test_predictions = model.transform(test_df)
        test_predictions = converter.transform(test_predictions)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        )
        accuracy = evaluator.evaluate(test_predictions)
        print(f"--- ACCURACY en test set: {accuracy:.4f} ({accuracy * 100:.2f}%) ---")

        model.write().overwrite().save(model_path)
        print(f"--- Modelo guardado en {model_path} ---")

        metrics_df = spark.createDataFrame(
            [(round(accuracy * 100, 2), train_df.count(), test_df.count())],
            ["accuracy", "train_size", "test_size"],
        ).withColumn("fecha", current_timestamp())

        metrics_df.write.format("mongodb").mode("overwrite").option(
            "database", "heart_db"
        ).option("collection", "metrics").option("writeConcern.w", "1").save()

        print("--- Métricas guardadas en MongoDB ---")

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()