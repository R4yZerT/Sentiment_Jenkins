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
                
                // Instalación de dependencias
                sh "docker exec -u root spark_master python3 -m pip install --upgrade pip"
                sh "docker exec -u root spark_master python3 -m pip install numpy pandas"
                
                // Permisos
                sh "docker exec -u root spark_master chmod 777 /opt/spark/work-dir/process_sentiment.py /opt/spark/data/dataset_sentimientos_500.csv"
            }
        }

        stage('Ejecución') {
            steps {
                echo "Ejecutando proceso Batch (CSV)..."
                sh """
                    docker exec spark_master /opt/spark/bin/spark-submit \
                    --master local[*] \
                    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
                    --conf "spark.mongodb.write.connection.uri=mongodb://sentiment_mongo:27017/sentiment_db.results" \
                    /opt/spark/work-dir/process_sentiment.py
                """
            }
        }
    }

    // El bloque post ahora está DENTRO de pipeline
    post {
        success {
            echo "¡Éxito! El proceso finalizó correctamente."
        }
        failure {
            echo "El pipeline falló."
        }
    }
}