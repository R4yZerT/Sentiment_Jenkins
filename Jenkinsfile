pipeline {
    agent any

    environment {
        SERVICE_NAME = "spark_master"
    }

    stages {
        stage('Limpieza y Arranque') {
            steps {
                echo 'Borrando rastros previos...'
                sh 'docker compose down -v --remove-orphans'
                sh 'docker compose up -d --build --force-recreate'
                
                echo 'Esperando a que Docker asigne nombres y rutas...'                
                script {
                    env.REAL_CONTAINER = sh(script: "docker ps --filter 'name=${SERVICE_NAME}' --format '{{.Names}}' | head -n 1", returnStdout: true).trim()
                    echo "Contenedor detectado: ${env.REAL_CONTAINER}"
                }
            }
        }

        stage('Inyección de Datos y Dependencias') {
            steps {
                echo "Preparando archivos en ${env.REAL_CONTAINER}..."
                
                // 1. Crear estructuras de carpetas
                sh "docker exec -u root ${env.REAL_CONTAINER} mkdir -p /opt/spark/work-dir /opt/spark/data"
                
                // 2. Inyectar el script Y el dataset (Esto mata el error PATH_NOT_FOUND)
                sh "docker cp process_sentiment.py ${env.REAL_CONTAINER}:/opt/spark/work-dir/process_sentiment.py"
                sh "docker cp dataset_sentimientos_500.csv ${env.REAL_CONTAINER}:/opt/spark/data/dataset_sentimientos_500.csv"
                
                // 3. Permisos y librerías
                sh "docker exec -u root ${env.REAL_CONTAINER} chmod -R 777 /opt/spark/data /opt/spark/work-dir"
                echo 'Instalando dependencias de Python...'
                sh "docker exec -u root ${env.REAL_CONTAINER} pip install numpy pandas"
            }
        }

        stage('Ejecución Spark') {
            steps {
                echo 'Lanzando proceso Spark Submit...'
                sh """
                    docker exec ${env.REAL_CONTAINER} /bin/bash -c "
                    if [ -f /opt/bitnami/spark/bin/spark-submit ]; then
                        /opt/bitnami/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
                        /opt/spark/work-dir/process_sentiment.py
                    else
                        /opt/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
                        /opt/spark/work-dir/process_sentiment.py
                    fi"
                """
            }
        }
    }

    post {
        success { echo '¡Excelente! Todo fluyó correctamente.' }
        failure { echo 'Hubo un error. Verifica que el archivo CSV esté en la raíz de tu Git.' }
    }
}