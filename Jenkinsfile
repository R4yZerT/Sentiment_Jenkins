pipeline {
    agent any

    environment {
        SPARK_MASTER = "spark://spark-master:7077"
        SPARK_SCRIPT = "process_sentiment.py"
    }

    stages {
        stage('1. Checkout') {
            steps {
                echo 'Descargando código...'
                checkout scm
            }
        }

        stage('2. Build API') {
            steps {
                echo 'Construyendo API...'
                sh 'docker compose build flask_api'
            }
        }

        stage('3. Infra & File Inject') {
            steps {
                echo 'Reiniciando infraestructura...'
                sh 'docker compose down -v --remove-orphans || true'
                sh 'docker compose up -d'
                sh 'sleep 15' // Damos un poco más de tiempo para que el sistema operativo del contenedor inicie
                script {
                    echo 'Preparando entorno de datos en Spark...'
                    // Creamos el directorio /opt/spark/data por si no existe
                    sh "docker exec -u root spark_master mkdir -p /opt/spark/data"
                    
                    echo 'Inyectando Dataset y Script...'
                    // Inyectamos el CSV (Asegúrate de que el nombre del archivo en tu repo sea exacto)
                        sh "docker cp dataset_sentimientos_500.csv spark_master:/opt/spark/data/dataset_sentimientos_500.csv"
                    
                    // Inyectamos el script de procesamiento
                    sh "docker cp ${SPARK_SCRIPT} spark_master:/opt/spark/work-dir/${SPARK_SCRIPT}"
                    sh "docker exec -u root spark_master chmod +x /opt/spark/work-dir/${SPARK_SCRIPT}"
                }
            }
        }

        stage('4. Spark Processing') {
            steps {
                echo 'Ejecutando procesamiento...'
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master ${SPARK_MASTER} \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
                    /opt/spark/work-dir/${SPARK_SCRIPT}
                """
            }
        }
    }

    post {
        failure {
            echo 'Limpiando contenedores...'
            sh 'docker rm -f sentiment_mongo spark_master spark_worker sentiment_api || true'
        }
    }
}