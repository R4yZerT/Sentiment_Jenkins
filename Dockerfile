# Usamos Jenkins como base porque es el que controlará el flujo
FROM jenkins/jenkins:lts

USER root

# 1. Instalación de dependencias del sistema y herramientas de red
RUN apt-get update && apt-get install -y \
    lsb-release \
    python3-pip \
    curl \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# 2. Instalar Docker CLI (para que Jenkins use el Docker de tu Mac)
# Esto soluciona el error 'docker-compose: not found'
RUN install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && apt-get install -y docker-ce-cli docker-compose-plugin

# 3. Instalación de librerías Python (Pandas/Numpy corregido)
# Usamos --break-system-packages porque las versiones nuevas de Debian/Ubuntu lo requieren
RUN pip3 install --no-cache-dir numpy pandas --break-system-packages

# 4. Plugins de Jenkins necesarios para tu Pipeline
RUN jenkins-plugin-cli --plugins "workflow-aggregator git docker-workflow"

# Importante: Agregamos el usuario jenkins al grupo docker (ID 999 suele ser el de Docker en Mac/Linux)
# Esto ayuda con los permisos del socket
RUN usermod -aG root jenkins

USER jenkins