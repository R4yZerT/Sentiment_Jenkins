pipeline {
    agent any

    environment {
        SPARK_MASTER = "spark://spark-master:7077"
        SPARK_SCRIPT = "process_sentiment.py"
    }

    stages {
        stage('1. Checkout') {
            steps {
                echo 'Descargando código del repositorio...'
                checkout scm
            }
        }

        stage('2. Build API') {
            steps {
                echo 'Construyendo imagen de la API Flask...'
                sh 'docker compose build flask_api'
            }
        }

        spark-master:
            image: apache/spark:3.5.0
            container_name: spark_master
            user: root
            command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
            ports:
                - "8081:8080"
                - "7077:7077"
            volumes:
                # Al apuntar a ./spark, Docker mapea el contenido de esa carpeta
                - ./spark:/opt/spark/work-dir:rw
                - ./data:/opt/spark/data:rw
            networks:
                - sentiment_network

        stage('4. Spark Processing') {
            steps {
                echo 'Iniciando procesamiento en Spark...'
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master ${SPARK_MASTER} \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
                    /opt/spark/work-dir/${SPARK_SCRIPT}
                """
            }
        }

        stage('5. Health Check') {
            steps {
                echo 'Verificando API...'
                sh 'curl -f http://localhost:5001/health || echo "API no respondió, pero continuando..."'
            }
        }
    }

    post {
        always {
            echo 'Finalizando ejecución...'
        }
        failure {
            echo 'Error detectado. Limpiando...'
            sh 'docker rm -f sentiment_mongo spark_master spark_worker sentiment_api || true'
        }
    }
}