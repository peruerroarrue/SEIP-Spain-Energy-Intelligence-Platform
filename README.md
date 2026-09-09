# SEIP — Spain Energy Intelligence Platform

TFM (Máster Big Data & Data Engineering, UCM/NTIC). Plataforma de ingeniería de datos end-to-end sobre el sistema eléctrico español: ingesta dual batch + streaming de REData/ESIOS, arquitectura Medallion (Bronze/Silver/Gold) sobre Delta Lake sirviendo un modelo de forecasting del precio horario de la electricidad gestionado con MLflow.

Desplegado y verificado contra infraestructura real, no solo en local: Azure Data Lake Storage Gen2 + Unity Catalog, un cluster de Databricks, Kafka gestionado en Confluent Cloud (SASL_SSL), y 2 Jobs de Databricks programados que orquestan todo el pipeline de punta a punta.

La especificación técnica completa (fuentes de datos, reglas de ingesta ya validadas, arquitectura, fases) vive fuera de este repo como documento de contexto de trabajo; las decisiones técnicas tomadas durante la implementación — incluidos los hallazgos reales al desplegar contra Azure/Databricks/Confluent Cloud — se registran en [DECISIONS.md](DECISIONS.md). El estado de avance pieza por pieza, con lo que queda pendiente, vive en [TODO.md](TODO.md).

## Arquitectura

```
REData API ──┐                                            ┌── Gold: KPIs (precio/hora, % renovable,
             ├─► Bronze (Delta) ─► Silver (Delta) ─► ──────┤            PVPC vs SPOT)
ESIOS API ───┤   schema-on-read    tipado, UTC,            └── Gold: feature store ─► modelos LightGBM
    │        │                     dedup, validado             (24, uno por horizonte h+1..h+24,
    ▼        │                                                  MLflow Model Registry)
Kafka producer ──► Confluent Cloud ──► Spark Structured           │
(run_once, cada        (4 topics)      Streaming (Bronze)         ▼
 5-15 min)                                                 Inferencia: forecast 24h ─► Gold
```

Todo el almacenamiento es Delta Lake sobre ADLS Gen2 (contenedores `bronze`/`silver`/`gold`), gobernado con Unity Catalog. Todo el cómputo es un cluster de Databricks (DBR 15.4 LTS / Spark 3.5.x). El pipeline completo corre programado como 2 Jobs de Databricks (ver más abajo), no como celdas de notebook ejecutadas a mano.

## Stack

| Componente | Tecnología |
|---|---|
| Ingesta streaming | Apache Kafka vía Confluent Cloud (SASL_SSL), `confluent-kafka` como producer |
| Ingesta batch | REData API, PySpark + Spark SQL |
| Procesamiento streaming | Spark Structured Streaming (`availableNow`, no always-on) |
| Almacenamiento | Delta Lake sobre Azure Data Lake Storage Gen2 |
| Lakehouse / gobernanza | Databricks sobre Azure, Unity Catalog |
| Calidad de datos | Motor de reglas propio (`seip.quality.validations`) — ver DECISIONS.md para por qué no Great Expectations ni DLT |
| ML | LightGBM + MLflow (tracking + Model Registry, Unity Catalog) |
| Orquestación | 2 Jobs de Databricks (Workflows), scripts `.py` versionados en este repo vía Git folder |
| CI | GitHub Actions (`tests.yml`), rama `main` protegida (PR + tests obligatorios) |
| Artefacto | Wheel Python (`pyproject.toml` + `setuptools`) |

## Estructura

```
src/seip/
├── ingestion/         # clientes REData/ESIOS, producer Kafka, jobs batch/streaming/backfill
├── transform/         # Bronze → Silver → Gold
├── quality/           # motor de validaciones
└── ml/                # features, entrenamiento (24 horizontes), inferencia
tests/                 # tests unitarios — lógica de negocio pura, sin SparkSession, corren en <1s
scripts/                # smoke tests manuales contra sistemas reales (no en la suite de pytest)
databricks_jobs/       # los 10 scripts que corren como tareas de los 2 Jobs de Databricks
databricks_manual/     # pasos manuales/bajo demanda (reentrenamiento, backfill histórico) — versionados igual, pero fuera de cualquier Job
```

## Pipeline en producción (Databricks Workflows)

**`seip-streaming-ingest`** (cada 5-15 min): productor ESIOS → Confluent Cloud → consumidor Spark → Bronze.

**`seip-batch-pipeline`** (diario): REData → Bronze → Silver, Silver ESIOS → hourly join → 2 KPIs de Gold + feature store → inferencia (forecast de 24h).

El entrenamiento (`train.py`) queda deliberadamente fuera de ambos Jobs — reentrenar automáticamente cada día sin criterio sobre la calidad/cantidad de datos nuevos no es MLOps razonable para este proyecto; es un paso manual/bajo demanda, coherente con el alcance de MLOps realista del spec (sin reentrenamiento automático, sin serving en tiempo real, sin monitorización de drift).

## Desarrollo local

```bash
pip install -e ".[dev]"
pytest
```

Grupos opcionales de dependencias (`streaming`, `spark`, `ml`) se instalan solo cuando se necesitan, para mantener el entorno de tests de lógica de negocio ligero — cada módulo separa su lógica pura (testeable sin SparkSession/MLflow/confluent-kafka) de la orquestación que sí depende de ellos.

### PySpark local en Windows

Esta máquina necesita **JDK 11** para correr Spark local — el JDK 17/21 del sistema falla al lanzar la JVM (bug de sockets AF_UNIX, ver DECISIONS.md). Antes de ejecutar cualquier script que use PySpark:

```powershell
$env:JAVA_HOME = "C:\java-tools\jdk-11.0.32+9"
```

### Kafka local (desarrollo/respaldo)

```bash
docker compose up -d
```

Sirve como entorno de desarrollo sin depender de que el cluster de Confluent Cloud esté activo — `kafka_producer.py`/`streaming_bronze.py` funcionan igual contra `localhost:9092` (PLAINTEXT) que contra Confluent Cloud (SASL_SSL), solo cambia la configuración pasada.
