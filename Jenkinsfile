pipeline {
    agent any

    environment {
        // Ajusta estas rutas según tu estructura final confirmada
        SPARK_MASTER = "spark://spark-master:7077"
        SPARK_SCRIPT = "/opt/spark/work-dir/process_sentiment.py"
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
                echo 'Limpiando y levantando infraestructura...'
                // Usamos || true para que el pipeline no muera si no hay nada que limpiar
                sh 'docker compose down --remove-orphans || true'
                sh 'docker compose up -d'
                
                script {
                    echo 'Esperando a que Spark Master esté listo...'
                    // IMPORTANTE: Escapamos las llaves con \ para que Groovy no las procese
                    sh "docker inspect -f '{{.State.Running}}' spark_master"
                }
            }
        }

        stage('4. Spark Processing') {
            steps {
                echo 'Iniciando procesamiento en Spark...'
                sh "docker exec spark_master /opt/spark/bin/spark-submit --master ${SPARK_MASTER} --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 ${SPARK_SCRIPT}"
            }
        }

        stage('5. Health Check') {
            steps {
                echo 'Verificando estado de los servicios...'
                sh 'curl -f http://localhost:5001/health || exit 1'
            }
        }
    }

    post {
        always {
            echo 'Finalizando ejecución...'
        }
        failure {
            echo 'Error detectado. Limpiando contenedores conflictivos...'
            // Limpieza automática si algo falla para no bloquear el siguiente build
            sh 'docker rm -f sentiment_mongo spark_master spark_worker sentiment_api || true'
        }
    }
}