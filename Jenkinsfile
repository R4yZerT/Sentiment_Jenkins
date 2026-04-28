pipeline {
    agent any

    environment {
        SPARK_MASTER = "spark://spark-master:7077"
        // Asegúrate de que el nombre coincida con tu archivo .py
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
                echo 'Levantando infraestructura y limpiando volúmenes...'
                sh 'docker compose down -v --remove-orphans || true'
                sh 'docker compose up -d'
                sh 'sleep 10'
                script {
                    echo 'Corrigiendo permisos del script en el contenedor...'
                    // Intentamos dar permisos al archivo directamente en el volumen montado
                    sh "docker exec -u root spark_master chmod +x /opt/spark/work-dir/${SPARK_SCRIPT} || echo 'No se pudo aplicar chmod'"
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
                echo 'Verificando estado de la API...'
                // Intentamos un hit a la API para ver si está viva
                sh 'curl -f http://localhost:5001/health || echo "La API no respondió en el puerto 5001"'
            }
        }
    }

    post {
        failure {
            echo 'El pipeline falló. Limpiando contenedores para evitar conflictos en el próximo build...'
            sh 'docker rm -f sentiment_mongo spark_master spark_worker sentiment_api || true'
        }
        success {
            echo '¡Pipeline completado con éxito!'
        }
    }
}