pipeline {
    agent any
    environment {
        // Nombre base que definiste en el compose
        SERVICE_NAME = "spark_master"
    }
    stages {
        stage('Limpieza Total') {
            steps {
                echo 'Borrando todo rastro de ejecuciones previas...'
                sh 'docker compose down -v --remove-orphans'
            }
        }

        stage('Levantar y Detectar') {
            steps {
                sh 'docker compose up -d --build --force-recreate'
                echo 'Esperando 20 segundos para que Spark inicie...'
                sleep 20
                
                // Esta línea busca el nombre real del contenedor para evitar el "No such container"
                script {
                    env.REAL_CONTAINER = sh(script: "docker ps --filter 'name=${SERVICE_NAME}' --format '{{.Names}}' | head -n 1", returnStdout: true).trim()
                    echo "Contenedor detectado: ${env.REAL_CONTAINER}"
                }
            }
        }

        stage('Inyección Forzada del Script') {
            steps {
                echo "Inyectando script en ${env.REAL_CONTAINER}..."
                // 1. Creamos la ruta por si no existe
                sh "docker exec -u root ${env.REAL_CONTAINER} mkdir -p /opt/spark/work-dir"
                // 2. Copiamos el archivo desde el workspace de Jenkins al contenedor
                sh "docker cp process_sentiment.py ${env.REAL_CONTAINER}:/opt/spark/work-dir/process_sentiment.py"
                // 3. Verificamos con LS para que TÚ lo veas en el log
                sh "docker exec ${env.REAL_CONTAINER} ls -la /opt/spark/work-dir/process_sentiment.py"
            }
        }

        stage('Ejecución') {
            steps {
                echo 'Iniciando Spark Submit con rutas absolutas...'
                sh """
                    docker exec ${env.REAL_CONTAINER} /bin/bash -c "
                    if [ -f /opt/bitnami/spark/bin/spark-submit ]; then
                        /opt/bitnami/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
                        /opt/spark/work-dir/process_sentiment.py
                    elif [ -f /opt/spark/bin/spark-submit ]; then
                        /opt/spark/bin/spark-submit \
                        --master spark://spark-master:7077 \
                        --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
                        /opt/spark/work-dir/process_sentiment.py
                    else
                        echo 'ERROR: No se encontró spark-submit en las rutas conocidas.'
                        exit 1
                    fi"
                """
            }
        }
    }
}