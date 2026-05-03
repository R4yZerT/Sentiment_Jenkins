pipeline {
    agent any

    environment {
        MASTER_CONTAINER = "spark_master"
    }

    stages {
        stage('Validación de Entorno') {
            steps {
                echo "Verificando que los contenedores estén activos..."
                // Si el contenedor no existe, esto fallará y detendrá el pipeline antes de intentar copiar
                sh "docker inspect ${env.MASTER_CONTAINER} > /dev/null"
            }
        }

        stage('Sincronización de Archivos') {
            steps {
                echo "Copiando scripts y datasets al Master..."
                // Aseguramos las carpetas
                sh "docker exec -u root ${env.MASTER_CONTAINER} mkdir -p /opt/spark/work-dir /opt/spark/data"
                
                // Copia los archivos
                sh "docker cp process_sentiment.py ${env.MASTER_CONTAINER}:/opt/spark/work-dir/process_sentiment.py"
                sh "docker cp dataset_sentimientos_500.csv ${env.MASTER_CONTAINER}:/opt/spark/data/dataset_sentimientos_500.csv"
                
                // Permisos
                sh "docker exec -u root ${env.MASTER_CONTAINER} chmod -R 777 /opt/spark/work-dir /opt/spark/data"
            }
        }

        stage('Ejecución Spark') {
            steps {
                echo "Ejecutando Spark Submit en local..."
                // Usamos --master local[*] para evitar problemas de conexión entre contenedores
                sh """
                    docker exec ${env.MASTER_CONTAINER} /opt/spark/bin/spark-submit \
                    --master local[*] \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                    /opt/spark/work-dir/process_sentiment.py
                """
            }
        }
    }

    post {
        success {
            echo "¡Éxito! El proceso finalizó correctamente."
        }
        failure {
            echo "El pipeline falló."
            echo "TIP: Ejecuta 'docker logs ${env.MASTER_CONTAINER}' para ver el error exacto de Spark."
        }
    }
}