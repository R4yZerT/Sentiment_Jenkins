pipeline {
    agent any

    environment {
        SERVICE_NAME = "spark_master"
    }

    stages { // <--- ESTO ES LO QUE TE FALTABA
        stage('Limpieza y Arranque') {
            steps {
                echo 'Borrando rastros previos...'
                sh 'docker compose down --remove-orphans || true'
                // Forzamos limpieza profunda
                sh 'docker ps -q --filter "name=spark_" | xargs -r docker rm -f'
                sh 'docker ps -q --filter "name=sentiment_" | xargs -r docker rm -f'
                
                echo 'Iniciando nuevo stack...'
                sh 'docker compose up -d --build --force-recreate'
            }
        }

        stage('Inyección de Datos y Dependencias') {
            steps {
                echo "Sincronizando archivos en todo el cluster..."
                script {
                    // 1. Identificar Master y Worker(s)
                    def containers = sh(script: "docker ps --filter 'name=spark' --format '{{.Names}}'", returnStdout: true).trim().split('\n')
                    
                    containers.each { container ->
                        echo "Configurando contenedor: ${container}"
                        // Crear carpetas
                        sh "docker exec -u root ${container} mkdir -p /opt/spark/work-dir /opt/spark/data"
                        // Copiar archivos
                        sh "docker cp process_sentiment.py ${container}:/opt/spark/work-dir/process_sentiment.py"
                        sh "docker cp dataset_sentimientos_500.csv ${container}:/opt/spark/data/dataset_sentimientos_500.csv"
                        // Permisos totales
                        sh "docker exec -u root ${container} chmod -R 777 /opt/spark/data /opt/spark/work-dir"
                        // Instalar dependencias (necesario en todos para que los workers procesen)
                        sh "docker exec -u root ${container} pip install numpy pandas"
                    }
                }
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
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                        /opt/spark/work-dir/process_sentiment.py
                    else
                        /opt/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
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
