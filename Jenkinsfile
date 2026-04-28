pipeline {
    agent any

    environment {
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_SCRIPT = "/opt/spark/work-dir/process_sentiment.py"
}
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
                    // --remove-orphans asegura que borre contenedores de intentos fallidos
                sh 'docker compose down --remove-orphans || true'
                sh 'docker compose up -d'
                
                script {
                    echo 'Esperando a que Spark Master esté listo...'
                    // Verificamos que el contenedor realmente subió
                        sh "docker inspect -f '{{.State.Running}}' spark_master"
                }
            }
        }

        stage('4. Spark Processing') {
            steps {
                echo 'Iniciando entrenamiento del modelo en Spark...'
                // Ejecutamos el script usando la ruta interna configurada
                sh """
                docker exec spark_master /opt/spark/bin/spark-submit \
                --master ${env.SPARK_MASTER} \
                --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
                ${env.SPARK_SCRIPT}
                """
            }
        }

        stage('5. Health Check') {
            steps {
                echo 'Verificando conectividad de la API...'
                // Reintento simple para la API
                sh 'sleep 5 && curl -f http://localhost:5001/stats || exit 1'
            }
        }
    }

    post {
        success {
            echo '¡Pipeline completado con éxito!'
        }
        failure {
            echo 'Error detectado. Revisa los logs de Docker con: docker logs spark_master'
        }
    }
}