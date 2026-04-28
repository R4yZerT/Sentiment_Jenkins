pipeline {
    agent any

    environment {
    SPARK_MASTER = "spark://spark-master:7077"
    // Esta es la ruta interna dentro del contenedor spark_master
    SPARK_SCRIPT = "/opt/spark/work-dir/scripts/process_sentiment.py" 
    IVY_OPTS = "-Divy.cache.dir=/tmp -Divy.home=/tmp"
}

    stages {
        stage('1. Checkout') {
            steps {
                echo 'Descargando código del repositorio...'
                // Jenkins descarga automáticamente el código aquí
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
                echo 'Limpiando y validando servicios...'
                sh 'docker compose down || true'
                sh 'docker compose up -d'
                // CRÍTICO: Esperar a que Spark termine de bootear
                sh 'sleep 15' 
            }
        }

        stage('4. Spark Processing') {
            steps {
                echo 'Iniciando entrenamiento...'
                // Verificamos si el contenedor sigue vivo antes de disparar
                sh 'docker ps' 
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
                // Esperamos unos segundos a que la API responda en el puerto 5001
                sh 'sleep 5 && curl -f http://localhost:5001/stats || exit 1'
            }
        }
    }

    post {
        success {
            echo '¡Pipeline completado con éxito! Datos listos para Power BI.'
        }
        failure {
            echo 'Hubo un error en el pipeline. Revisa los logs de Spark o Docker.'
        }
    }
}