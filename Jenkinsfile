pipeline {
    agent any

    environment {
        // Nombre del servicio definido en tu docker-compose.yml
        SERVICE_NAME = "spark_master"
    }

    stages {
        stage('Limpieza Total') {
            steps {
                echo 'Borrando contenedores y volúmenes previos para evitar basura de datos...'
                sh 'docker compose down -v --remove-orphans'
            }
        }

        stage('Levantar Infraestructura') {
            steps {
                echo 'Levantando servicios y detectando nombre real del contenedor...'
                sh 'docker compose up -d --build --force-recreate'
                
                echo 'Esperando 20 segundos para que Spark y Mongo estabilicen...'
                sleep 20
                
                script {
                    // Detecta si el contenedor se llama spark_master o pipe-spark_master-1
                    env.REAL_CONTAINER = sh(script: "docker ps --filter 'name=${SERVICE_NAME}' --format '{{.Names}}' | head -n 1", returnStdout: true).trim()
                    echo "Trabajando sobre el contenedor: ${env.REAL_CONTAINER}"
                }
            }
        }

        stage('Inyección de Script y Dependencias') {
            steps {
                echo "Preparando el entorno interno de ${env.REAL_CONTAINER}..."
                
                // 1. Crear carpeta y copiar el script (soluciona el error 'No such file')
                sh "docker exec -u root ${env.REAL_CONTAINER} mkdir -p /opt/spark/work-dir"
                sh "docker cp process_sentiment.py ${env.REAL_CONTAINER}:/opt/spark/work-dir/process_sentiment.py"
                
                // 2. Instalar librerías de Python (soluciona el error 'ModuleNotFoundError: numpy')
                echo 'Instalando numpy y pandas dentro de Spark...'
                sh "docker exec -u root ${env.REAL_CONTAINER} pip install numpy pandas"
                
                // 3. Verificación visual en los logs
                sh "docker exec ${env.REAL_CONTAINER} ls -la /opt/spark/work-dir/process_sentiment.py"
            }
        }

        stage('Ejecución Spark') {
            steps {
                echo 'Iniciando procesamiento de sentimientos...'
                // Usamos rutas absolutas de Bitnami para evitar el error 'command not found'
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
        success {
            echo '¡POR FIN! El pipeline terminó correctamente.'
        }
        failure {
            echo 'Algo volvió a fallar. Revisa los logs de arriba detalladamente.'
        }
    }
}