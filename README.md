# Big Data Sentiments — Sentiment Analysis + Heart Failure Prediction

Pipeline de Big Data con dos módulos: **Análisis de Sentimientos** (batch) y **Predicción de Insuficiencia Cardíaca** (streaming con Kafka), procesados con Apache Spark, almacenados en MongoDB, expuestos mediante APIs REST en Flask y visualizados en un dashboard interactivo con Streamlit. Orquestado con Jenkins CI/CD.

---

## Arquitectura

```
                              ┌──────────────────────────────────────────┐
                              │              Jenkins CI/CD               │
                              │  Sincronización → Limpieza DB →          │
                              │  Sentiment Batch → Heart Model Training   │
                              └──────┬────────────────────┬─────────────┘
                                     │                    │
                          spark-submit             spark-submit
                                     │                    │
                    ┌────────────────▼───┐    ┌──────────▼──────────────┐
                    │  Sentiment Pipeline │    │  Heart Model Training   │
                    │  (process_sentiment)│    │  (train_heart_model)    │
                    │  TF-IDF + RF        │    │  OneHot + RF            │
                    │  Split 70/30         │    │  Split 80/20            │
                    └────────┬────────────┘    └──────────┬──────────────┘
                             │                            │
                             ▼                            ▼ salva modelo
                    ┌─────────────────────┐    ┌──────────────────────┐
                    │   sentiment_db       │    │  heart_model (spark) │
                    │   .results           │    │  heart_db            │
                    │   .metrics           │    │  .metrics            │
                    └────────┬─────────────┘    └──────────▲───────────┘
                             │                              │
              ┌──────────────┴──────────────┐    ┌───────────┴────────────┐
              │                             │    │   Heart Consumer       │
              │   Flask API (:5001)          │    │   (process_heart.py)   │
              │   /sentiments, /stats        │    │   Spark Streaming →    │
              │                               │    │   Kafka → MongoDB       │
              └──────────────┬──────────────┘    │   restart: always      │
                             │                    └─────────▲──────────────┘
                             │                              │
              ┌──────────────┴──────────────┐    ┌─────────┴────────┐
              │   Heart Flask API (:5002)     │    │   Kafka Topic    │
              │   /patients, /predictions     │    │  heart-records   │
              │   /stats, /risk-summary        │    └────────▲────────┘
              └──────────────┬──────────────┘              │
                             │                    ┌─────────┴────────┐
                             │                    │  Kafka Producer  │
                             │                    │  (kafka/producer)│
                             │                    │  Lee heart.csv   │
                             │                    │  1 msg/segundo   │
                             │                    └─────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │   Dashboard Streamlit (:8502) │
              │   Tab 1: Sentiment Dashboard  │
              │   Tab 2: Sentiment Stats API  │
              │   Tab 3: Heart Dashboard       │
              │   Tab 4: Heart Predictions     │
              └───────────────────────────────┘
```

---

## Servicios Docker

| Contenedor | Imagen | Puerto | Rol |
|---|---|---|---|
| `sentiment_mongo` | mongo:latest | 27017 | Base de datos (sentiment_db + heart_db) |
| `kafka` | apache/kafka:3.7.0 | 29092 | Broker Kafka en modo KRaft |
| `heart_producer` | personalizado | — | Envía registros de heart.csv a Kafka |
| `heart_consumer` | personalizado | — | Spark Streaming: Kafka → predicción → MongoDB |
| `spark_master` | apache/spark:3.5.0 | 8081, 7077 | Master de Spark |
| `spark_worker` | apache/spark:3.5.0 | — | Worker de Spark |
| `sentiment_api` | python:3.11-slim | 5001 | API REST Flask (sentimiento) |
| `heart_api` | python:3.11-slim | 5002 | API REST Flask (heart) |
| `sentiment_dashboard` | python:3.11-slim | 8502 | Dashboard Streamlit |
| `sentiment_jenkins` | jenkins/jenkins:lts | 8085 | Orquestador CI/CD |

---

## Estructura del proyecto

```
.
├── docker-compose.yml
├── Jenkinsfile
├── Dockerfile                           # Dockerfile de Jenkins
├── process_sentiment.py                 # Spark batch: sentimiento
├── train_heart_model.py                 # Spark batch: entrena modelo heart
├── process_heart.py                     # Spark streaming: consume Kafka → predice → MongoDB
├── data/
│   ├── dataset_sentimientos_500.csv
│   └── heart.csv                        # 918 registros de Heart Failure Prediction
├── kafka/
│   ├── producer.py                      # Envía heart.csv a Kafka (1 msg/seg)
│   ├── Dockerfile
│   └── requirements.txt
├── heart_consumer/
│   └── Dockerfile                       # Spark + numpy para el consumer
├── heart_api/
│   ├── app.py                           # Flask API: /patients, /predictions, /stats, /risk-summary
│   ├── Dockerfile
│   └── requirements.txt
├── api/
│   ├── app.py                           # Flask API: /sentiments, /stats, /predict
│   ├── Dockerfile
│   └── requirements.txt
├── dashboard/
│   ├── app.py                           # Streamlit dashboard (4 tabs)
│   ├── Dockerfile
│   └── requirements.txt
└── prepare_dataset.py                   # Enriquece CSV de sentimientos con IDs y fechas
```

---

## Módulo 1: Sentiment Analysis (Batch)

Pipeline NLP + ML en Spark que se ejecuta como job batch desde Jenkins.

### Pipeline

1. Lee `dataset_sentimientos_500.csv`
2. Split **70/30** (train/test, seed=42)
3. Pipeline de ML:
   - `Tokenizer` → `StopWordsRemover` → `HashingTF` → `IDF` → `StringIndexer` → `RandomForestClassifier`
4. `IndexToString` aplicado manualmente con labels del StringIndexer fitteado
5. Evalúa sobre test set → guarda accuracy en `sentiment_db.metrics`
6. Predice sobre todo el dataset → guarda en `sentiment_db.results`

### Campos en MongoDB

| Campo | Descripción |
|---|---|---|
| `texto` | Texto original |
| `etiqueta` | Etiqueta real (positivo/negativo/neutral) |
| `prediction` | Predicción del modelo |
| `fecha_proceso` | Timestamp de procesamiento |
| `accuracy_test` | Accuracy del test set (%) |

---

## Módulo 2: Heart Failure Prediction (Streaming)

Pipeline de streaming con Kafka que predice riesgo de insuficiencia cardíaca en tiempo real.

### Flujo

```
heart.csv → Kafka Producer → Kafka Topic (heart-records)
                                      │
                              Heart Consumer (Spark Streaming)
                                      │
                               PipelineModel.load()
                                      │
                              → MongoDB (heart_db.predictions)
                                      │
                              → Flask API (:5002)
                                      │
                              → Dashboard (tab 3 y 4)
```

### train_heart_model.py (Batch)

1. Lee `heart.csv` (918 registros)
2. Split **80/20** (train/test, seed=42)
3. Pipeline: `StringIndexer` ×5 → `OneHotEncoder` ×5 → `VectorAssembler` → `StringIndexer(label)` → `RandomForest(100 trees)`
4. Evalúa sobre test set → accuracy ~85.91%
5. Guarda modelo en `/opt/spark/models/heart_model`
6. Guarda métricas en `heart_db.metrics`

### Heart Consumer (process_heart.py)

Servicio persistente (`restart: always`) que ejecuta Spark Streaming:

1. Lee de Kafka topic `heart-records` (`startingOffsets=earliest`)
2. Carga `PipelineModel` desde `/opt/spark/models/heart_model`
3. Transforma cada micro-batch
4. Convierte `prediction` a integer (0=Sano, 1=Riesgo)
5. Guarda en `heart_db.predictions` con `foreachBatch`
6. Usa checkpoint en volumen `heart_checkpoints` para recuperación ante fallos

### Campos en MongoDB

| Campo | Tipo | Descripción |
|---|---|---|
| `Age` | int | Edad |
| `Sex` | str | Sexo (M/F) |
| `ChestPainType` | str | Tipo de dolor torácico |
| `RestingBP` | int | Presión arterial en reposo |
| `Cholesterol` | int | Colesterol |
| `FastingBS` | int | Glucemia en ayunas (0/1) |
| `RestingECG` | str | ECG en reposo |
| `MaxHR` | int | Frecuencia cardíaca máxima |
| `ExerciseAngina` | str | Angina inducida por ejercicio (Y/N) |
| `Oldpeak` | float | Depresión ST |
| `ST_Slope` | str | Pendiente ST |
| `heart_disease_label` | int | Etiqueta real (0=Sano, 1=Riesgo) |
| `prediction` | int | Predicción del modelo (0=Sano, 1=Riesgo) |
| `fecha_proceso` | datetime | Timestamp de procesamiento |

---

## APIs REST

### Sentiment API — `http://localhost:5001`

| Endpoint | Método | Descripción |
|---|---|---|
| `/sentiments` | GET | Últimos 50 registros. Filtro: `?type=positivo` |
| `/stats` | GET | Distribución de clases y accuracy |
| `/predict` | POST | Inferencia sobre texto nuevo |

### Heart API — `http://localhost:5002`

| Endpoint | Método | Descripción |
|---|---|---|
| `/patients` | GET | Últimos 50 pacientes |
| `/predictions` | GET | Predicciones. Filtro: `?risk=0` o `?risk=1` |
| `/stats` | GET | Distribución y accuracy |
| `/risk-summary` | GET | Resumen: total, riesgo alto/bajo, accuracy |

---

## Dashboard — `http://localhost:8502`

| Tab | Contenido |
|---|---|
| **Sentiment Dashboard** | KPIs, distribución pie, etiqueta vs predicción, accuracy, timeline, tabla filtrable |
| **Sentiment Stats API** | Métricas de /stats, listado de /sentiments con filtro |
| **Heart Dashboard** | KPIs (total, riesgo, sano), distribución pie, riesgo por tipo de dolor, timeline |
| **Heart Predictions** | Resumen de riesgo, tabla /patients, predicciones filtradas por riesgo |

---

## Cómo correr el proyecto

### Requisitos previos
- Docker Desktop instalado y corriendo
- Git

### 1. Clonar el repositorio
```bash
git clone https://github.com/R4yZerT/Sentiment_Jenkins.git
cd Sentiment_Jenkins
```

### 2. Preparar el dataset enriquecido
```bash
python3 prepare_dataset.py
```

### 3. Levantar todos los servicios
```bash
docker compose up -d --build
```

### 4. Verificar que todo está corriendo
```bash
docker ps
```

Deberías ver 10 contenedores: `sentiment_mongo`, `kafka`, `heart_producer`, `heart_consumer`, `spark_master`, `spark_worker`, `sentiment_api`, `heart_api`, `sentiment_dashboard`, `sentiment_jenkins`.

### 5. Configurar Jenkins

El volumen de Jenkins se configura fuera del repositorio (`~/jenkins_home`) para evitar problemas de I/O con OneDrive.

- **URL**: `http://localhost:8085`
- **Usuario**: `perezyeiver`
- **Contraseña**: `perezyeiver123`

Para crear un Pipeline:
1. **New Item** → Pipeline
2. **Pipeline** → **Pipeline script from SCM**
3. SCM: Git, URL del repositorio, branch `*/main`
4. Script path: `Jenkinsfile`
5. **Build Now**

### 6. Ejecutar Jenkins pipeline

Esto entrena ambos modelos (sentimiento + heart) y guarda en MongoDB. El consumer de streaming se reinicia automáticamente si hay cambios.

### 7. Verificar datos en MongoDB
```bash
# Sentimiento
docker exec sentiment_mongo mongosh sentiment_db --eval "db.results.countDocuments({})"

# Heart
docker exec sentiment_mongo mongosh heart_db --eval "db.predictions.countDocuments({})"
```

### 8. Abrir el dashboard
```
http://localhost:8502
```

---

## URLs de acceso

| Servicio | URL |
|---|---|
| Dashboard | http://localhost:8502 |
| Sentiment API | http://localhost:5001 |
| Heart API | http://localhost:5002 |
| Jenkins | http://localhost:8085 |
| Spark UI | http://localhost:8081 |
| Kafka | localhost:29092 |

---

## Comandos útiles

```bash
# Ver logs de un servicio
docker logs -f heart_producer
docker logs -f heart_consumer
docker logs spark_master

# Ver logs del consumidor de streaming (servicio persistente)
docker logs -f heart_consumer

# Reiniciar un servicio
docker compose restart heart_api

# Reconstruir y reiniciar un servicio específico
docker compose up -d --build heart_consumer

# Reiniciar el producer de Kafka
docker compose restart producer

# Limpiar datos de MongoDB
docker exec sentiment_mongo mongosh heart_db --eval 'db.predictions.deleteMany({})'
docker exec sentiment_mongo mongosh sentiment_db --eval 'db.results.deleteMany({})'

# Apagar todo
docker compose down

# Apagar y eliminar volúmenes (borra MongoDB, modelos y checkpoints)
docker compose down -v
```

---

## Tecnologías

| Tecnología | Uso |
|---|---|
| Apache Spark 3.5.0 | Procesamiento batch y streaming |
| Apache Kafka 3.7.0 (KRaft) | Streaming de datos en tiempo real |
| MongoDB | Almacenamiento de resultados |
| Flask | APIs REST |
| Streamlit | Dashboard interactivo |
| Jenkins | CI/CD y orquestación |
| Docker / Docker Compose | Contenerización |
| Python | Lógica de aplicación |
| PySpark ML | Pipelines de Machine Learning |

---

## Dataset

### Sentimiento
`dataset_sentimientos_500.csv` — 500 textos en inglés etiquetados como positivo/negativo/neutral.

### Heart Failure Prediction
`heart.csv` — 918 registros de pacientes con 11 features clínicos y etiqueta binaria (0=Sano, 1=Enfermedad cardíaca). Fuente: [Kaggle](https://www.kaggle.com/datasets/fedesoriano/heart-failure-prediction).
