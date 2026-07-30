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
- [ ] `batch_job.py` — no empezado

## Transform

- [ ] `bronze_to_silver.py` — no empezado
- [ ] `silver_to_gold.py` — no empezado
- [ ] Pipeline DLT real (los `@dlt.table`/`@dlt.expect` que envuelven la lógica pura de Silver) — no se puede probar en local, requiere desplegar a un workspace de Databricks

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

## Documentación / entrega

- [ ] Memoria
- [ ] Vídeo demo (máx. 5 min)
- [ ] Diagrama de arquitectura
