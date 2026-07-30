# Diario de decisiones técnicas

Registro breve de decisiones no triviales y su motivo, para sostener la memoria y la defensa oral del TFM.

## 2026-07-28 — Scaffolding inicial del repo

- Layout `src/seip/` (src-layout) en vez de paquete plano en la raíz, para evitar imports accidentales del código sin instalar y facilitar el empaquetado como wheel.
- `pyproject.toml` con dependencias opcionales separadas por grupo (`streaming`, `spark`, `ml`) y `requests` como única dependencia base. Motivo: la lógica de negocio pura (parseo de API, transformaciones) debe poder testearse con `pip install -e ".[dev]"` sin instalar PySpark/Databricks/MLflow, para que la suite de tests sea rápida y no dependa de un cluster.
- Se pospone la creación de `.github/workflows/batch-ingestion.yml` hasta que exista un job de batch real que ejecutar (Fase 2/3) — crear el workflow antes sería un placeholder sin valor.

## 2026-07-30 — Kafka producer (Fase 2)

- **Kafka local en Docker (KRaft, sin Zookeeper) en vez de Confluent Cloud desde el principio.** Motivo: evita depender de que el free tier de Confluent esté despierto mientras se itera, y no consume la cuota gratuita. El producer lee `KAFKA_BOOTSTRAP_SERVERS`/`KAFKA_SECURITY_PROTOCOL` de variables de entorno, así que apuntar a Confluent Cloud más adelante es solo cambiar config, no código.
- **Serialización JSON plano, no Avro + Schema Registry.** El spec no especifica formato — fue una decisión abierta. Se descartó Avro porque su valor principal (forzar contratos de esquema entre equipos independientes que no se coordinan) no aplica aquí: un único autor escribe el producer y el consumidor (job Bronze) en el mismo repo. Añadir Schema Registry habría sido una pieza de infraestructura más sin resolver un problema real del proyecto, justo el riesgo de "colección de tecnologías" que señalaron los tutores.
- **Ventana de fetch con solapamiento (últimos ~25 min) en vez de cursor exacto por indicador.** Más simple y tolera reinicios del producer sin perder datos, a costa de duplicados esperados en Bronze — se resuelven en la deduplicación de Silver por `(indicator_id, datetime_utc)`, ya prevista en el spec.
- **Un solo proceso con un loop de 5 min** (la granularidad más fina, eólica/solar) en vez de 4 procesos o un scheduler dedicado — cada indicador se publica solo cuando su intervalo nativo lo exige (PVPC cada hora, SPOT cada 15 min).
- Hallazgo de la prueba real: la API de ESIOS **sí acepta `start_date`/`end_date` con hora** (no solo fecha), lo cual no estaba confirmado — sin esto la ventana de polling habría tenido que traer el día completo en cada tick.
- Verificado end-to-end contra ESIOS real + Kafka local (`scripts/smoke_kafka_producer.py`): un tick produce y se puede consumir correctamente en los 4 topics. Pendiente: soak test de 48h (criterio de aceptación de la Fase 2) y prueba contra Confluent Cloud real.

## 2026-07-30 — REData client

- Mismo patrón que `esios_client.py` (retry/backoff, tests con mocks + smoke test real), pero sin filtro de `geo_id` — REData no mezcla países en la misma respuesta como sí hace ESIOS con el indicador SPOT.
- Parser dedicado para la forma anidada de `balance-electrico` (`attributes.content[].attributes.values` en vez de `attributes.values` directo) — verificado contra datos reales (41 valores, múltiples títulos).
- `intercambios` se deja como endpoint no crítico: se probó contra la API real y falló tras 3 reintentos (500/503), tal y como el spec advertía. No se trata como error bloqueante en el diseño del futuro `batch_job.py`.
- No se ha extraído la lógica de retry compartida entre `esios_client.py` y `redata_client.py` (queda algo duplicada) — se revisará si aparece un tercer cliente con la misma necesidad; no se ha hecho ahora para no refactorizar código ya mergeado sin necesidad concreta.

## 2026-07-30 — batch_job.py y hallazgo de entorno: PySpark local requiere JDK 11 en esta máquina

- Bronze definido como "raw JSON + metadatos de ingesta" (`ingestion_date`, `source`, `fetched_at`, `raw_json`), sin tipar — coherente con la definición de Bronze del spec (schema-on-read) y con lo ya hecho en streaming (los mensajes de Kafka tampoco se tipan, solo se envuelven). El tipado/casteo se deja para Silver.
- `intercambios` (Francia/Portugal/Marruecos) se trata como fuente no crítica dentro del batch: si falla, se loguea y se continúa — falló efectivamente en la prueba real, tal y como el spec advertía.
- **Hallazgo de entorno (no del proyecto):** PySpark no arrancaba en esta máquina con JDK 17 (Eclipse Adoptium) ni JDK 21 — la JVM fallaba al crear una pipe de loopback interna vía Unix Domain Sockets (`SocketException: Invalid argument: connect` en `sun.nio.ch.UnixDomainSockets`), algo que las builds recientes de esos JDKs usan también en Windows. Se resolvió instalando **Eclipse Temurin JDK 11** (`C:\java-tools\jdk-11.0.32+9`, zip portable, sin instalador) y apuntando `JAVA_HOME` ahí — JDK 11 no usa esa optimización y conecta por TCP loopback normal. Cualquier trabajo con PySpark local en esta máquina debe usar ese JDK 11 vía `JAVA_HOME`, no el JDK 17/21 del sistema.
- Verificado end-to-end con `scripts/smoke_batch_job.py`: 75 filas escritas y leídas correctamente en una tabla Delta local particionada por `ingestion_date` (30 generación + 4 renovable/no-renovable + 41 balance).
