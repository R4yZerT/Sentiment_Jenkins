FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
# Instalamos numpy y pandas directamente desde los repositorios de Debian
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-numpy \
    python3-pandas \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Si necesitas algo que NO esté en los repos de Debian, usa pip con el flag
RUN pip3 install --no-cache-dir pymongo --break-system-packages
COPY . .
CMD ["python", "app.py"]