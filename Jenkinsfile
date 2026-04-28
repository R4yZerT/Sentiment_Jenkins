pipeline {
    agent any

    environment {
        SPARK_MASTER = "spark_master"
    }

    stages {
        stage('Limpieza y Preparación') {
            steps {
                echo 'Limpiando contenedores previos...'
                // Usamos "docker compose" sin guion
                sh 'docker compose down -v --remove-orphans'
            }
        }

        stage('Levantar Infraestructura') {
            steps {
                echo 'Levantando servicios...'
                // Forzamos recreación para asegurar que tome los cambios del .py
                sh 'docker compose up -d --build --force-recreate'
                
                echo 'Esperando inicialización...'
                sleep 20
            }
        }

        stage('Verificación de Código') {
            steps {
                echo '--- Código actual en el contenedor ---'
                sh "docker exec ${SPARK_MASTER} cat /opt/spark/work-dir/process_sentiment.py"
            }
        }

        stage('Ejecutar Procesamiento Spark') {
            steps {
                echo 'Iniciando Spark Submit...'
                sh """
                    docker exec ${SPARK_MASTER} spark-submit \
                    --master spark://spark-master:7077 \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
                    /opt/spark/work-dir/process_sentiment.py
                """
            }
        }
    }

    post {
        failure {
            echo 'El pipeline falló. Verifica si Jenkins tiene acceso al binario de docker.'
        }
    }
}