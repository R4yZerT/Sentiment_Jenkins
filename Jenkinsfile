pipeline {
    agent any

    environment {
    SPARK_MASTER = "spark://spark-master:7077"
    SPARK_SCRIPT = "process_sentiment.py" // Verifica si es con 's' o sin 's'
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
                 echo 'Levantando infraestructura...'
                sh 'docker compose down --remove-orphans || true'
                sh 'docker compose up -d'   
                script {
                    echo 'Buscando el script en el contenedor...'
                    sh "docker exec spark_master find /opt/spark/work-dir -name process_sentiment.py"
                }
            }
        }

        stage('4. Spark Processing') {
    steps {
        echo 'Iniciando procesamiento en Spark...'
        // Cambiamos la ruta al archivo a simplemente el nombre si está en el work-dir
        sh "docker exec spark_master /opt/spark/bin/spark-submit \
            --master ${SPARK_MASTER} \
            --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
            /opt/spark/work-dir/${SPARK_SCRIPT}"
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