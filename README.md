# Sentiment Analysis Pipeline

Pipeline de análisis de sentimientos con procesamiento batch en Apache Spark, orquestación con Jenkins, almacenamiento en MongoDB, API REST en Flask y dashboard interactivo en Streamlit.

---

## Arquitectura

```
Jenkins (CI/CD)
    └── spark-submit process_sentiment.py
            ├── Lee dataset_sentimientos_500.csv
            ├── Split 70/30 train/test
            ├── Entrena modelo (TF-IDF + Random Forest) → Accuracy ~--% en test set
            └── Escribe predicciones → MongoDB
                        ├── Flask API  (puerto 5001)
                        └── Dashboard  (puerto 8502)
```

### Servicios Docker

| Contenedor | Imagen | Puerto | Rol |
|---|---|---|---|
| `sentiment_mongo` | mongo:latest | 27017 | Base de datos de resultados |
| `sentiment_api` | python:3.11-slim | 5001 | API REST Flask |
| `sentiment_dashboard` | python:3.11-slim | 8502 | Dashboard Streamlit |
| `spark_master` | apache/spark:3.5.0 | 8081, 7077 | Nodo master de Spark |
| `spark_worker` | apache/spark:3.5.0 | — | Nodo worker de Spark |
| `sentiment_jenkins` | jenkins/jenkins:lts | 8085 | Orquestador CI/CD |

---

## Estructura del proyecto

```
.
├── Dockerfile                  # Imagen de Jenkins con Docker CLI y Python
├── Jenkinsfile                 # Pipeline CI/CD (2 stages: sincronización + ejecución)
├── docker-compose.yml          # Orquestación de todos los servicios
├── process_sentiment.py        # Job de Spark: entrena modelo y guarda en MongoDB
├── prepare_dataset.py          # Script auxiliar: enriquece el CSV con IDs y fechas
├── dataset_sentimientos_500.csv # Dataset original (500 textos etiquetados)
├── dataset_final.csv     # Dataset con IDs y fechas simuladas (Nov 2025 - Feb 2026)
├── app.py                      # API Flask simple (endpoint /resultados)
├── api/
│   ├── app.py                  # API Flask completa (3 endpoints)
│   ├── Dockerfile
│   └── requirements.txt
└── dashboard/
    ├── app.py                  # Dashboard Streamlit (3 tabs)
    ├── Dockerfile
    └── requierement.txt
```

---

## Dataset

El archivo `dataset_sentimientos_500.csv` contiene 500 textos en inglés con su etiqueta de sentimiento:

| Campo | Descripción |
|---|---|
| `texto` | Texto a clasificar |
| `etiqueta` | Etiqueta real: `positivo`, `negativo`, `neutral` |

El script `prepare_dataset.py` genera `dataset_final.csv` agregando:
- `id`: identificador único (1-500)
- `fecha`: timestamp simulado distribuido entre Nov 2025 y Feb 2026

---

## Pipeline de ML (`process_sentiment.py`)

El job de Spark ejecuta el siguiente pipeline cada vez que Jenkins lo dispara:

1. Lee `dataset_sentimientos_500.csv`
2. Divide el dataset en **50% train / 50% test** (seed=42 para reproducibilidad)
3. Construye un pipeline de NLP + ML:
   - `Tokenizer` → tokeniza el texto en palabras
   - `StopWordsRemover` → elimina palabras vacías (the, is, a...)
   - `HashingTF` → vectorización (1000 features)
   - `IDF` → pondera las palabras más informativas
   - `StringIndexer` → convierte etiquetas a números (negativo=0, neutral=1, positivo=2)
   - `RandomForestClassifier` → clasificador con 100 árboles, profundidad máxima 10
4. Entrena el modelo **solo con el set de train**
5. Evalúa el modelo sobre el **set de test** → **Accuracy ~93%**
6. Predice sobre todo el dataset y guarda en MongoDB:
   - `sentiment_db.results` → predicciones con etiqueta en texto (positivo/negativo/neutral)
   - `sentiment_db.metrics` → accuracy, train size y test size del último run

> **Nota:** El modelo se entrena de nuevo en cada ejecución. Las métricas reales del test set se guardan en MongoDB y se muestran en el dashboard.

---

## API REST (`api/app.py`)

Base URL: `http://localhost:5001`

### `GET /sentiments`
Retorna los últimos 50 registros. Acepta filtro opcional por tipo.

```bash
curl http://localhost:5001/sentiments
curl "http://localhost:5001/sentiments?type=positivo"
```

Respuesta:
```json
[
  {
    "texto": "Amazing experience with the service",
    "etiqueta": "positivo",
    "prediction": "positivo",
    "fecha_proceso": "2026-05-06T20:59:31.805Z"
  }
]
```

### `GET /stats`
Retorna distribución de clases y accuracy del modelo.

```bash
curl http://localhost:5001/stats
```

Respuesta:
```json
{
  "total": 502,
  "distribucion": {
    "positivo": 168,
    "negativo": 167,
    "neutral": 167
  },
  "accuracy": 98.5
}
```

### `POST /predict`
Inferencia sobre texto nuevo usando un modelo sklearn (TF-IDF + Logistic Regression) entrenado al iniciar la API.

```bash
curl -X POST http://localhost:5001/predict \
  -H "Content-Type: application/json" \
  -d '{"texto": "This product is absolutely amazing!"}'
```

Respuesta:
```json
{
  "texto": "This product is absolutely amazing!",
  "prediction": "positivo",
  "confianza": {
    "positivo": 0.8921,
    "negativo": 0.0612,
    "neutral": 0.0467
  }
}
```

---

## Dashboard (`dashboard/app.py`)

Acceso: `http://localhost:8502`

Tiene 3 tabs:

| Tab | Contenido |
|---|---|
| **Dashboard** | KPIs, gráfica de distribución, etiqueta vs predicción, accuracy, timeline, tabla filtrable |
| **Stats API** | Métricas del endpoint `/stats` y listado de `/sentiments` con filtro |
| **Predictor** | Caja de texto para llamar a `/predict` en tiempo real con gráfica de confianza |

---

## Cómo correr el proyecto

### Requisitos previos
- Docker Desktop instalado y corriendo
- Git

### 1. Clonar el repositorio
```bash
git clone <url-del-repo>
cd Sentiment_Jenkins
```

### 2. Preparar el dataset enriquecido
```bash
python3 prepare_dataset.py
```

### 3. Levantar todos los servicios
```bash
docker-compose up -d --build
```

### 4. Verificar que todo está corriendo
```bash
docker ps
```

### 5. Configurar Jenkins (primera vez)

Obtener la contraseña inicial:
```bash
docker exec sentiment_jenkins cat /var/jenkins_home/secrets/initialAdminPassword
```

Abrir `http://localhost:8085`, pegar la contraseña y:
1. Instalar los plugins sugeridos
2. Crear un nuevo Pipeline
3. En **Definition** seleccionar `Pipeline script from SCM`
4. SCM: `Git`, URL del repositorio, branch `*/main`
5. Script path: `Jenkinsfile`
6. Guardar y ejecutar **Build Now**

### 6. Verificar datos en MongoDB
```bash
docker exec -it sentiment_mongo mongosh sentiment_db --eval "db.results.countDocuments()"
```

### 7. Abrir el dashboard
```
http://localhost:8502
```

---

## URLs de acceso

| Servicio | URL |
|---|---|
| Dashboard | http://localhost:8502 |
| API REST | http://localhost:5001 |
| Jenkins | http://localhost:8085 |
| Spark UI | http://localhost:8081 |

---

## Comandos útiles

```bash
# Ver logs de un servicio
docker logs sentiment_api
docker logs sentiment_dashboard
docker logs sentiment_jenkins

# Reiniciar un servicio específico
docker-compose restart flask_api

# Reconstruir y reiniciar un servicio
docker-compose up -d --build dashboard

# Apagar todo
docker-compose down

# Apagar y eliminar volúmenes (borra MongoDB)
docker-compose down -v
```
