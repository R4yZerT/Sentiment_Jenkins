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

        sstage('3. Infra & File Inject') {
            steps {
                echo 'Reiniciando infraestructura...'
                sh 'docker compose down -v --remove-orphans || true'
                sh 'docker compose up -d'
                sh 'sleep 15' // Damos un poco más de tiempo para que el sistema operativo del contenedor inicie
                script {
                    echo 'Instalando dependencias de Python en Spark (Master y Worker)...'
                    // Instalamos pip y numpy. Es vital que el Worker también lo tenga por si Spark distribuye la tarea.
                    sh "docker exec -u root spark_master apt-get update && docker exec -u root spark_master apt-get install -y python3-pip"
                    sh "docker exec -u root spark_master pip3 install numpy pandas"
                    
                    sh "docker exec -u root spark_worker apt-get update && docker exec -u root spark_worker apt-get install -y python3-pip"
                    sh "docker exec -u root spark_worker pip3 install numpy pandas"

                    echo 'Inyectando script...'
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