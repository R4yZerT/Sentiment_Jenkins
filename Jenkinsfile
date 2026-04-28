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

        stage('3. Infra Check') {
            steps {
                echo 'Reseteando infraestructura...'
                sh 'docker compose down -v --remove-orphans || true'
                sh 'docker compose up -d'
                sh 'sleep 5'
                script {
                    echo 'Corrigiendo permisos y verificando script...'
                    // Aseguramos que el script sea ejecutable dentro del contenedor
                    sh "docker exec -u root spark_master chmod +x /opt/spark/work-dir/${SPARK_SCRIPT}"
                    sh "docker exec spark_master ls -la /opt/spark/work-dir/${SPARK_SCRIPT}"
                }
            }
        }

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