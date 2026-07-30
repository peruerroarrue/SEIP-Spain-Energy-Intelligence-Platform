# Pendiente

Lista viva de lo que falta por implementar o por verificar contra sistemas reales (no solo tests con mocks). Se actualiza en paralelo al desarrollo, no al final.

## Ingestion

- [x] `esios_client.py` — fetch con retry/backoff, filtro geo_id, guarda indicador descontinuado (tests con mocks, ver `tests/test_esios_client.py`)
- [x] `esios_client.py` — probado contra la API real con token válido (`scripts/smoke_esios.py`, indicador 1001, 25 valores/día, geo_id 8741 correcto). Nota: al imprimir en consola Windows (Git Bash), caracteres como `€`/`í` salen mal (`Precio /MWh`) — confirmado que es solo la consola (codepoint `0x20ac` correcto en memoria), no un bug del cliente
- [ ] `esios_client.py` — leer `api_key` desde variable de entorno en vez de solo parámetro explícito
- [ ] `esios_client.py` — helper de paginación para backfill histórico largo (Regla 3: paginar por mes/año, pausa de cortesía 1-1.5s)
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
- [x] `streaming_bronze.py` — probado end-to-end con Kafka local + PySpark (`scripts/smoke_streaming_bronze.py`): 25 filas correctas en los 4 topics. Encontrado y corregido: `fetched_at` salía sin zona horaria (naive) porque Spark usaba el timezone por defecto de la JVM — fijado `spark.sql.session.timeZone=UTC` en la sesión local
- [ ] `streaming_bronze.py` — pendiente: probar con trigger continuo (no solo `availableNow`) si se quiere algo más "tiempo real" para la demo

## Transform

- [x] `bronze_to_silver.py` — hecho, **solo para el Bronze streaming ESIOS** (PVPC/SPOT/eólica/solar): parseo+tipado nativo Spark, UTC, dedup por `(indicator_id, datetime_utc)` con watermark de 2h, validaciones de rango (precio 0-700€, solar nocturna >10MW) como flags, no como descarte
- [x] `bronze_to_silver.py` — probado end-to-end (`scripts/smoke_bronze_to_silver.py`): parseo/tipado correcto contra datos reales, y **dedup verificado inyectando un duplicado real** (40 filas Bronze → 39 Silver, la fila superviviente queda marcada `value_out_of_range_0.0_700.0`). Nota: en una ejecución incremental, una fila añadida a Bronze por un proceso externo (no por los jobs habituales) no se recogió hasta limpiar el checkpoint y rehacer una pasada completa — rareza de estado incremental con escrituras fuera de los jobs normales, no debería reproducirse en el pipeline real (todo escrito por los mismos jobs programados)
- [x] `bronze_to_silver.py` — Silver para REData batch (generación/balance/renovable-no-renovable): `run_redata`, batch simple (no streaming, no hace falta watermark al no ser una fuente streaming), dedup por `(source, title, datetime_utc)`, UTC normalizado a partir del `datetime` con offset (REData no trae `datetime_utc` como ESIOS). Sin reglas de validación de rango — el spec no da ninguna para REData y no se ha inventado ninguna
- [x] `bronze_to_silver.py` — REData Silver probado end-to-end (`scripts/smoke_bronze_to_silver_redata.py`): 75 filas Bronze → 75 Silver (sin dupes reales), y **dedup verificado inyectando un duplicado real** (77 → 76, colapsa correctamente)
- [x] Funciones ESIOS renombradas con sufijo `_esios` (`parse_esios_bronze_record`, `run_esios`, etc.) al añadir REData al mismo fichero, para que no se confundan los dos pipelines
- [x] `bronze_to_silver.py` — join temporal hora a hora (`run_esios_hourly_join`): media por hora de cada serie (PVPC ya horario, SPOT/eólica/solar se promedian) + pivot a tabla ancha (`pvpc_eur_mwh`/`spot_eur_mwh`/`eolica_mw`/`solar_mw`), una fila por hora. Horas con algún indicador ausente quedan con `NULL` en esa columna, no se descartan
- [x] `bronze_to_silver.py` — join horario probado end-to-end (`scripts/smoke_bronze_to_silver_hourly_join.py`) contra datos reales: 4 filas horarias, SPOT/eólica/solar correctamente promediados dentro de cada hora, `NULL` correcto en horas con datos parciales
- [ ] `silver_to_gold.py` — no empezado
- [ ] Pipeline DLT real (los `@dlt.table`/`@dlt.expect` que envuelven la lógica pura de Silver) — no se puede probar en local, requiere desplegar a un workspace de Databricks. `bronze_to_silver.py` está escrito para poder envolverse en `@dlt.table` más adelante sin rehacer la lógica

## Quality

- [ ] `validations.py` — no empezado
- [ ] Decisión Great Expectations vs expectativas nativas DLT — pendiente de evaluar

## ML

- [ ] `features.py`, `train.py`, `inference.py` — no empezados

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
