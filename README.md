# SEIP — Spain Energy Intelligence Platform

TFM (Máster Big Data & Data Engineering, UCM/NTIC). Plataforma de ingeniería de datos end-to-end sobre el sistema eléctrico español: ingesta batch + streaming de REData/ESIOS, arquitectura Medallion (Bronze/Silver/Gold) sobre Delta Lake, y un modelo de forecasting del precio horario servido con MLflow.

La especificación técnica completa (fuentes de datos, reglas de ingesta ya validadas, arquitectura, fases) vive fuera de este repo como documento de contexto de trabajo; las decisiones técnicas tomadas durante la implementación se registran en [DECISIONS.md](DECISIONS.md).

## Estructura

```
src/seip/
├── ingestion/    # clientes REData/ESIOS, producer Kafka, job batch
├── transform/    # Bronze → Silver → Gold
├── quality/      # validaciones de datos
└── ml/           # features, entrenamiento, inferencia
tests/            # tests unitarios (lógica de negocio pura, sin SparkSession)
```

## Desarrollo

```bash
pip install -e ".[dev]"
pytest
```

Grupos opcionales de dependencias (`streaming`, `spark`, `ml`) se instalan solo cuando se necesitan, para mantener el entorno de tests de lógica de negocio ligero.
