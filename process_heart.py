import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.ml import PipelineModel


def main():
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "heart-records")
    model_path = os.environ.get("MODEL_PATH", "/opt/spark/models/heart_model")
    mongo_uri = os.environ.get(
        "MONGO_URI", "mongodb://sentiment_mongo:27017/heart_db.predictions"
    )
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "/opt/spark/checkpoints/heart")

    spark = (
        SparkSession.builder.appName("HeartStreamConsumer")
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[stream] Cargando modelo desde {model_path}...")
    model = PipelineModel.load(model_path)
    print("[stream] Modelo cargado.")
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString


def main():
    kafka_brokers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "heart-records")
    model_path = os.environ.get("MODEL_PATH", "/opt/spark/models/heart_model")
    mongo_uri = os.environ.get(
        "MONGO_URI", "mongodb://sentiment_mongo:27017/heart_db.predictions"
    )
    checkpoint_path = os.environ.get("CHECKPOINT_PATH", "/opt/spark/checkpoints/heart")

    spark = (
        SparkSession.builder.appName("HeartStreamConsumer")
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[stream] Cargando modelo desde {model_path}...")
    model = PipelineModel.load(model_path)
    print("[stream] Modelo cargado.")

    # Obtener labels del StringIndexer del label (stage del label_indexer)
    # Pipeline stages: 5 indexers + 5 encoders + assembler + label_indexer + rf = 13 stages
    # label_indexer es el stage 11 (index 11)
    n_cat = 5
    label_indexer_idx = n_cat + n_cat + 1  # indexers + encoders + assembler
    fitted_label_indexer = model.stages[label_indexer_idx]
    heart_labels = fitted_label_indexer.labelsArray[0]
    print(f"[stream] Label mapping: {list(enumerate(heart_labels))}")

    converter = IndexToString(
        inputCol="prediction",
        outputCol="prediction_label",
        labels=heart_labels,
    )

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
        raw_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_brokers)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed_df = raw_df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        predictions_df = model.transform(parsed_df)

        output_cols = [
            "Age",
            "Sex",
            "ChestPainType",
            "RestingBP",
            "Cholesterol",
            "FastingBS",
            "RestingECG",
            "MaxHR",
            "ExerciseAngina",
            "Oldpeak",
            "ST_Slope",
            "HeartDisease",
            "prediction",
        ]

        result_df = (
            predictions_df.select(output_cols)
            .withColumn("fecha_proceso", current_timestamp())
            .withColumn("prediction", col("prediction").cast("int"))
            .withColumnRenamed("HeartDisease", "heart_disease_label")
        )

        def write_to_mongo(batch_df, batch_id):
            count = batch_df.count()
            if count > 0:
                batch_df.write.format("mongodb").mode("append").option(
                    "database", "heart_db"
                ).option("collection", "predictions").option(
                    "writeConcern.w", "1"
                ).save()
                print(f"[stream] Batch {batch_id}: {count} registros guardados en MongoDB")

        query = result_df.writeStream.foreachBatch(write_to_mongo).outputMode(
            "append"
        ).trigger(processingTime="10 seconds").start()

        print("[stream] Consumiendo de Kafka. Presiona Ctrl+C para detener.")
        query.awaitTermination()

    except Exception as e:
        print(f"--- ERROR CRÍTICO ---: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()