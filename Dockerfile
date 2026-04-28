FROM apache/spark:3.5.0
FROM jenkins/jenkins:lts

USER root

# Instala los plugins básicos y los de MongoDB/Spark de una vez
RUN jenkins-plugin-cli --plugins "workflow-aggregator git docker-workflow"

# Instalamos pip y las librerías para que el script no falle por falta de numpy
# Instalamos pip y luego las librerías con el flag de desbloqueo
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir numpy pandas --break-system-packages

# Volvemos al usuario spark para que el proceso tenga los permisos correctos
USER spark