pipeline {
    agent any

    environment {
        // Definimos el nombre del contenedor del master de Spark para los comandos exec
        SPARK_MASTER = "spark_master"
    }

    stages {
        stage('Limpieza y Preparación') {
            steps {
                echo 'Deteniendo contenedores previos y limpiando volúmenes...'
                // -v borra los volúmenes, eliminando metadatos de esquemas viejos de Spark
                sh 'docker-compose down -v --remove-orphans'
            }
        }

        stage('Levantar Infraestructura') {
            steps {
                echo 'Levantando servicios (MongoDB, Spark Master/Worker, API)...'
                // --build fuerza a Docker a reconstruir las imágenes si hubo cambios en los archivos
                sh 'docker-compose up -d --build --force-recreate'
                
                echo 'Esperando a que los servicios estén listos (15s)...'
                sleep 15
            }
        }

        stage('Verificación de Código') {
            steps {
                echo '--- CONTENIDO DEL ARCHIVO DENTRO DEL CONTENEDOR ---'
                // Este paso es vital: si el log de Jenkins te muestra aquí el código VIEJO, 
                // entonces el problema está en tu configuración de volúmenes de Docker.
                sh "docker exec ${SPARK_MASTER} cat /opt/spark/work-dir/process_sentiment.py"
            }
        }

        stage('Ejecutar Procesamiento Spark') {
            steps {
                echo 'Enviando tarea a Spark Cluster...'
                // Ejecutamos el script dentro del master
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
            echo 'El pipeline falló. Revisa los logs de Spark arriba.'
        }
        always {
            echo 'Pipeline finalizado.'
            // Opcional: Descomenta la siguiente línea si quieres apagar todo al terminar
            // sh 'docker-compose down'
        }
    }
}