# Pendiente

Lista viva de lo que falta por implementar o por verificar contra sistemas reales (no solo tests con mocks). Se actualiza en paralelo al desarrollo, no al final.

## Ingestion

- [x] `esios_client.py` — fetch con retry/backoff, filtro geo_id, guarda indicador descontinuado (tests con mocks, ver `tests/test_esios_client.py`)
- [x] `esios_client.py` — probado contra la API real con token válido (`scripts/smoke_esios.py`, indicador 1001, 25 valores/día, geo_id 8741 correcto). Nota: al imprimir en consola Windows (Git Bash), caracteres como `€`/`í` salen mal (`Precio /MWh`) — confirmado que es solo la consola (codepoint `0x20ac` correcto en memoria), no un bug del cliente
- [ ] `esios_client.py` — leer `api_key` desde variable de entorno en vez de solo parámetro explícito
- [x] `esios_client.py` — helper de paginación para backfill histórico largo (Regla 3: paginar por mes/año, pausa de cortesía 1-1.5s) — hecho en `historical_backfill.py` (`month_windows`), no en el propio cliente
- [x] `redata_client.py` — hecho: `fetch_series` genérico + wrappers para generación/renovable-no-renovable/demanda/balance/intercambios, con parser de `content` anidado para balance-electrico
- [x] `redata_client.py` — probado contra la API real (`scripts/smoke_redata.py`): generación (30 valores, 15 fuentes), renovable/no-renovable, balance (41 valores, parser de `content` verificado con datos reales). `intercambios` falló tras reintentos como se esperaba (endpoint no crítico)
- [ ] `redata_client.py` — leer credenciales/params desde config en vez de solo argumentos explícitos (mismo pendiente que ESIOS, no es crítico ahora mismo)
- [x] `kafka_producer.py` — hecho (Fase 2). JSON plano (decisión documentada, ver DECISIONS.md), ventana con solapamiento + dedup delegado a Silver, config Kafka local Docker vs Confluent Cloud vía variables de entorno
- [x] `kafka_producer.py` — probado end-to-end contra ESIOS real + Kafka local en Docker (`scripts/smoke_kafka_producer.py`): 1 tick, 4 topics, mensajes consumidos de vuelta correctamente
- [ ] `kafka_producer.py` — pendiente: soak test de 48h sin caídas (criterio de aceptación Fase 2), todavía no ejecutado
- [ ] `kafka_producer.py` — pendiente: probar configuración contra Confluent Cloud real (de momento solo Docker local)
- [x] `batch_job.py` — hecho: `build_tasks`/`fetch_all`/`to_bronze_rows` (lógica pura) + `run` (Spark/Delta), `intercambios` no crítico
- [x] `batch_job.py` — probado end-to-end con PySpark local + Delta Lake (`scripts/smoke_batch_job.py`): 75 filas escritas y releídas correctamente, particionado por `ingestion_date`. Requiere JDK 11 en esta máquina (ver DECISIONS.md — JDK 17/21 fallan por un bug de sockets AF_UNIX)
- [x] `streaming_bronze.py` — hecho: consume los 4 topics ESIOS con Spark Structured Streaming (`availableNow`), aterriza en `data/bronze/esios` con el mismo esquema que el batch (`ingestion_date`/`source`/`fetched_at`/`raw_json`)
- [x] `streaming_bronze.py` — probado end-to-end con Kafka local + PySpark (`scripts/smoke_streaming_bronze.py`): 25 filas correctas en los 4 topics
- [x] **Corrección importante:** el arreglo original de `fetched_at` (fijar `spark.sql.session.timeZone=UTC`) era incompleto — `.collect()` de un `TimestampType` en PySpark ignora esa config y usa la zona horaria local de la JVM. `fetched_at` llevaba hasta 2h de más. Arreglado castenado a epoch antes de recoger (ver DECISIONS.md). Regla general para todo el proyecto: nunca `.collect()` un `TimestampType` directamente
- [ ] `streaming_bronze.py` — pendiente: probar con trigger continuo (no solo `availableNow`) si se quiere algo más "tiempo real" para la demo

## Transform

- [x] `bronze_to_silver.py` — hecho, **solo para el Bronze streaming ESIOS** (PVPC/SPOT/eólica/solar): parseo+tipado nativo Spark, UTC, dedup por `(indicator_id, datetime_utc)` con watermark de 2h, validaciones de rango (precio 0-700€, solar nocturna >10MW) como flags, no como descarte
- [x] `bronze_to_silver.py` — probado end-to-end (`scripts/smoke_bronze_to_silver.py`): parseo/tipado correcto contra datos reales, y **dedup verificado inyectando un duplicado real** (40 filas Bronze → 39 Silver, la fila superviviente queda marcada `value_out_of_range_0.0_700.0`). Nota: en una ejecución incremental, una fila añadida a Bronze por un proceso externo (no por los jobs habituales) no se recogió hasta limpiar el checkpoint y rehacer una pasada completa — rareza de estado incremental con escrituras fuera de los jobs normales, no debería reproducirse en el pipeline real (todo escrito por los mismos jobs programados)
- [x] `bronze_to_silver.py` — Silver para REData batch (generación/balance/renovable-no-renovable): `run_redata`, batch simple (no streaming, no hace falta watermark al no ser una fuente streaming), dedup por `(source, title, datetime_utc)`, UTC normalizado a partir del `datetime` con offset (REData no trae `datetime_utc` como ESIOS). Sin reglas de validación de rango — el spec no da ninguna para REData y no se ha inventado ninguna
- [x] `bronze_to_silver.py` — REData Silver probado end-to-end (`scripts/smoke_bronze_to_silver_redata.py`): 75 filas Bronze → 75 Silver (sin dupes reales), y **dedup verificado inyectando un duplicado real** (77 → 76, colapsa correctamente)
- [x] Funciones ESIOS renombradas con sufijo `_esios` (`parse_esios_bronze_record`, `run_esios`, etc.) al añadir REData al mismo fichero, para que no se confundan los dos pipelines
- [x] `bronze_to_silver.py` — join temporal hora a hora (`run_esios_hourly_join`): media por hora de cada serie (PVPC ya horario, SPOT/eólica/solar se promedian) + pivot a tabla ancha (`pvpc_eur_mwh`/`spot_eur_mwh`/`eolica_mw`/`solar_mw`), una fila por hora. Horas con algún indicador ausente quedan con `NULL` en esa columna, no se descartan
- [x] `bronze_to_silver.py` — join horario probado end-to-end (`scripts/smoke_bronze_to_silver_hourly_join.py`) contra datos reales: 4 filas horarias, SPOT/eólica/solar correctamente promediados dentro de cada hora, `NULL` correcto en horas con datos parciales
- [x] `silver_to_gold.py` — 2 de los 3 KPIs del spec: precio medio por hora del día (`avg_pvpc_eur_mwh`/`avg_spot_eur_mwh`) y comparativa PVPC vs SPOT (`spread_eur_mwh`), ambos a partir del Silver horario ESIOS. `% penetración renovable` queda pendiente (necesita cruzar REData día + ESIOS hora, decisión propia sin resolver todavía)
- [x] `silver_to_gold.py` — probado end-to-end (`scripts/smoke_silver_to_gold.py`) contra datos reales: `NULL` correcto donde falta un precio, spread calculado bien donde hay ambos. Nota: con los datos de prueba actuales (pocas horas de un solo día) no se llega a ejercitar el promedio entre *varios* días para una misma hora — eso sí está cubierto por los tests unitarios en Python puro
- [ ] `silver_to_gold.py` — **PENDIENTE, aparcado deliberadamente el 2026-07-30 para priorizar `train.py` (núcleo > enriquecimiento Gold, per spec sección 2). Retomar con esto:**
  - Fuentes ya listas para el cruce: `data/silver/redata` (filas `source=evolucion_renovable_no_renovable`, `title` "Renovable"/"No renovable", granularidad **día**) + `data/silver/esios_hourly` (granularidad **hora**)
  - Decisión de diseño pendiente de tomar: cómo repartir el % diario de REData a cada una de las 24 horas de ese día (opción más simple: repetir el mismo % en todas las horas del día — un "broadcast join" por fecha en vez de por hora exacta)
  - Una vez decidido, añadir como: (a) columna en un nuevo KPI Gold `% penetración renovable`, y (b) feature adicional en `seip/ml/features.py` ("% renovable de la última hora", sección 6 del spec)
- [ ] Pipeline DLT real (los `@dlt.table`/`@dlt.expect` que envuelven la lógica pura de Silver) — no se puede probar en local, requiere desplegar a un workspace de Databricks. `bronze_to_silver.py` está escrito para poder envolverse en `@dlt.table` más adelante sin rehacer la lógica

## Quality

- [ ] `validations.py` — no empezado
- [ ] Decisión Great Expectations vs expectativas nativas DLT — pendiente de evaluar

## ML

- [x] `features.py` — hecho: lags de PVPC (1h/24h/168h, vía join por timestamp desplazado, no `lag()` posicional — no se descoloca si hay huecos), variables cíclicas hora-del-día/día-de-la-semana (seno/coseno), mes del año. `% renovable` y demanda quedan pendientes — ver la nota detallada de `silver_to_gold.py` en Transform, arriba
- [x] `features.py` — probado end-to-end (`scripts/smoke_features.py`), incluyendo un cross-check exacto Spark-vs-Python de las variables cíclicas ("all rows match exactly") — fue precisamente esta verificación la que sacó a la luz el bug de zona horaria de `streaming_bronze.py`
- [x] `historical_backfill.py` — hecho: backfill paginado por mes de los 4 indicadores ESIOS a `data/bronze/esios` (mismo esquema que streaming), reutiliza `kafka_producer.INDICATORS` como fuente única de verdad
- [x] `historical_backfill.py` — **ejecutado de verdad contra la API real**: 6 meses (2026-02-06 a 2026-08-05), 125.279 filas Bronze → 125.255 Silver → 4.320 filas horarias (180 días × 24h exacto)
- [x] `train.py` — hecho: 24 modelos independientes por horizonte (h+1..h+24, no recursivo), target + variables cíclicas recalculadas para la **hora objetivo** (no la hora de origen — importa para el ciclo diario), baseline naive (`pvpc_lag_24h`), split cronológico train/test (nunca aleatorio en series temporales), tracking MLflow (params/métricas/modelo)
- [x] `train.py` — **los 24 modelos se registran en el Model Registry**, uno por horizonte (`seip-pvpc-forecast-h1`...`h24`), cada uno con alias `reference` — no solo h+1. Necesario para que `inference.py` pueda generar el forecast completo de 24h, no solo 1 hora vista. Confirmado: 24/24 registrados correctamente (`h1` en v2 por reentreno, `h2`-`h24` en v1)
- [x] `train.py` — **entrenado y evaluado contra datos reales de verdad** (`scripts/smoke_train.py`, split de 3 meses de test): **los 24/24 horizontes superan el baseline naive en RMSE y MAE** (p.ej. h+1: RMSE 29.43 vs 55.28€; h+24: RMSE 32.70 vs 62.76€) — cumple el criterio de aceptación de la Fase 4. Modelo `seip-pvpc-forecast-h1` v1 registrado con alias `reference` confirmado
- [x] Corregido en el camino: `mlflow.sklearn.log_model` falla con LightGBM en MLflow 3.x (skops no confía por defecto en `lightgbm.basic.Booster`) — usar `mlflow.lightgbm.log_model` en su lugar
- [ ] `inference.py` — no empezado

## Infraestructura / soporte

- [x] Repo scaffolding, `pyproject.toml`, CI `tests.yml`
- [x] Branching GitHub Flow + protección de rama `main` (require PR + pytest check)
- [ ] `.github/workflows/batch-ingestion.yml` (cron diario) — pospuesto hasta que exista un batch job real
- [ ] Decisión Unity Catalog vs Hive Metastore — pendiente de probar en el workspace de Databricks
- [ ] Confluent Cloud vs Kafka local — configurar y documentar en DECISIONS.md
- [ ] Verificar que el wheel se instala limpio en un entorno nuevo (criterio de aceptación Fase 5)
- [x] PySpark local funcionando en esta máquina: usar JAVA_HOME=`C:\java-tools\jdk-11.0.32+9` (JDK 17/21 del sistema fallan, ver DECISIONS.md)

## Documentación / entrega

- [ ] Memoria
- [ ] Vídeo demo (máx. 5 min)
- [ ] Diagrama de arquitectura
