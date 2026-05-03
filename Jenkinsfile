pipeline {
    agent any

    stages {
        stage('Sincronización') {
            steps {
                echo "Sincronizando archivos y dependencias..."
                sh "docker exec -u root spark_master mkdir -p /opt/spark/work-dir /opt/spark/data"
                
                // Copia los archivos
                sh "docker cp process_sentiment.py spark_master:/opt/spark/work-dir/process_sentiment.py"
                sh "docker cp dataset_sentimientos_500.csv spark_master:/opt/spark/data/dataset_sentimientos_500.csv"
                
                // INSTALACIÓN FORZADA DE DEPENDENCIAS
                // Usamos --user por si el usuario no es root, y nos aseguramos de que pip esté actualizado
                sh "docker exec -u root spark_master python3 -m pip install --upgrade pip"
                sh "docker exec -u root spark_master python3 -m pip install numpy pandas"
                
                // Permisos
                sh "docker exec -u root spark_master chmod 777 /opt/spark/work-dir/process_sentiment.py /opt/spark/data/dataset_sentimientos_500.csv"
            }
        }

        stage('Ejecución') {
            steps {
                echo "Ejecutando Spark..."
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master local[*] \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                    /opt/spark/work-dir/process_sentiment.py
                """
            }
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
