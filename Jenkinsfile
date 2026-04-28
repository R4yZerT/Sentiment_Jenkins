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
        echo 'Levantando infraestructura...'
        sh 'docker compose down || true'
        sh 'docker compose up -d'
        
        script {
            echo 'Esperando a que Spark Master esté listo...'
            // waitUntil reintenta el bloque hasta que retorne 'true'
            waitUntil {
                def status = sh(script: "docker inspect -f '{{.State.Running}}' spark_master", returnStdout: true).trim()
                return status == 'true'
            }
        }
        echo 'Spark Master está oficialmente arriba.'
        sh 'sleep 10' // Respiro final para que el proceso interno de Spark inicie
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