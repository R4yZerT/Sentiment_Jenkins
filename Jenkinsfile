pipeline {
    agent any

    stages {
        stage('Sincronización') {
            steps {
                echo "Sincronizando archivos y dependencias..."
                sh "docker exec -u root spark_master mkdir -p /opt/spark/work-dir /opt/spark/data /opt/spark/models"
                
                // Copiar archivos de sentimiento
                sh "docker cp process_sentiment.py spark_master:/opt/spark/work-dir/process_sentiment.py"
                sh "docker cp data/dataset_sentimientos_500.csv spark_master:/opt/spark/data/dataset_sentimientos_500.csv"
                
                // Copiar archivos de heart
                sh "docker cp train_heart_model.py spark_master:/opt/spark/work-dir/train_heart_model.py"
                sh "docker cp process_heart.py spark_master:/opt/spark/work-dir/process_heart.py"
                sh "docker cp data/heart.csv spark_master:/opt/spark/data/heart.csv"
                
                // Instalación de dependencias
                sh "docker exec -u root spark_master python3 -m pip install --upgrade pip"
                sh "docker exec -u root spark_master python3 -m pip install numpy pandas"
                
                // Permisos solo en archivos copiados (evita .git montado desde host)
                sh "docker exec -u root spark_master chmod 777 /opt/spark/work-dir/process_sentiment.py /opt/spark/work-dir/train_heart_model.py /opt/spark/work-dir/process_heart.py"
                sh "docker exec -u root spark_master chmod 777 /opt/spark/data/dataset_sentimientos_500.csv /opt/spark/data/heart.csv"
            }
        }

        stage('Limpieza MongoDB') {
            steps {
                echo "Limpiando colecciones en MongoDB..."
                sh "docker exec sentiment_mongo mongosh sentiment_db --eval 'db.results.deleteMany({})'"
                sh "docker exec sentiment_mongo mongosh heart_db --eval 'db.predictions.deleteMany({})'"
                sh "docker exec sentiment_mongo mongosh heart_db --eval 'db.metrics.deleteMany({})'"
            }
        }

        stage('Sentiment Batch') {
            steps {
                echo "Ejecutando Sentiment Analysis..."
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master local[*] \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                    /opt/spark/work-dir/process_sentiment.py
                """
            }
        }

        stage('Heart Model Training') {
            steps {
                echo "Entrenando modelo Heart Failure Prediction..."
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master local[*] \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                    /opt/spark/work-dir/train_heart_model.py
                """
            }
        }
    }

    post {
        success {
            echo "¡Éxito! Todos los procesos finalizaron correctamente."
        }
        failure {
            echo "El pipeline falló."
        }
    }
}