# Diario de decisiones técnicas

Registro breve de decisiones no triviales y su motivo, para sostener la memoria y la defensa oral del TFM.

## 2026-07-28 — Scaffolding inicial del repo

- Layout `src/seip/` (src-layout) en vez de paquete plano en la raíz, para evitar imports accidentales del código sin instalar y facilitar el empaquetado como wheel.
- `pyproject.toml` con dependencias opcionales separadas por grupo (`streaming`, `spark`, `ml`) y `requests` como única dependencia base. Motivo: la lógica de negocio pura (parseo de API, transformaciones) debe poder testearse con `pip install -e ".[dev]"` sin instalar PySpark/Databricks/MLflow, para que la suite de tests sea rápida y no dependa de un cluster.
- Se pospone la creación de `.github/workflows/batch-ingestion.yml` hasta que exista un job de batch real que ejecutar (Fase 2/3) — crear el workflow antes sería un placeholder sin valor.
