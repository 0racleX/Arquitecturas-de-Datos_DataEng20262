# 4. Incrementalidad, idempotencia y recuperación

## Lote: cómo se identifica y cuándo se marca como procesado
- Un **lote** es la respuesta de una fuente en una ejecución: `(source_id, run_id)`. El `run_id` es
  el de Airflow (`scheduled__…`, `manual__…`) saneado para S3.
- **Bronze**: el lote queda registrado cuando se escribe su manifiesto en
  `control/ingestion_log/…/source=<f>.json`, **después** del XML. El manifiesto es la marca de
  commit de la ingesta. Si el manifiesto ya está en `SUCCESS`, reintentar la tarea no vuelve a
  consultar el RSS.
- **Silver**: el lote se marca como procesado con
  `control/silver/batches/source=<f>/run_id=<id>/seq=<n>.json`. Ese marcador se escribe **solo
  después del commit de Silver** (patrón outbox). Un lote sin marcador sigue pendiente y lo toma la
  siguiente ejecución.

## Nueva extracción vs. reprocesamiento
| Modo del DAG | Consulta RSS | Qué procesa |
|---|---|---|
| `extract` (programado) | Sí | Lotes nuevos + cualquier lote pendiente de los últimos 7 días |
| `reprocess` + `reprocess_run_id` | No | Ese lote desde Bronze (verificando su SHA-256), aunque ya esté procesado |
| `rebuild` | No | Todo Bronze desde cero; reconstruye Silver y Gold |

Reprocesar no crea otra copia lógica del lote: Bronze nunca se reescribe y la fusión por `news_id`
deja `unchanged` lo que ya existía. El marcador del lote reprocesado se registra con
`reprocessed = true`.

## Cómo se escriben y publican los archivos (sin duplicados ni parciales en Athena)
1. **Datos versionados**: cada partición de fecha que cambió se escribe en un prefijo nuevo e
   inmutable `…/pub_date=D/v=<run_id>/`. Athena todavía no lo ve: Glue sigue apuntando a la versión anterior.
2. **Compuerta de calidad** en memoria. Si falla, no se publica nada.
3. **Commit**: se reescribe `control/silver/current.json` (o `control/gold/current.json`) con la
   versión vigente de cada partición. Es una sola escritura atómica en S3.
4. **Outbox**: historial, cuarentena y marcadores de lote se copian de `work/` a su ruta final.
5. **Glue** (`glue_update_catalog`): cada partición pasa a apuntar a su versión vigente con
   `BatchCreatePartition` / `BatchUpdatePartition` / `BatchDeletePartition`. Athena pasa de una
   versión completa a otra; nunca mezcla archivos de dos ejecuciones dentro de una partición.
6. **Limpieza**: se borran versiones que no son la vigente ni la anterior.

Si el proceso cae antes del paso 3, la ejecución no existió para Athena. Si cae entre 3 y 5, la
siguiente tarea o ejecución completa el outbox y la sincronización de Glue. Reintentar con el mismo
`run_id` escribe en las mismas claves.

**Concurrencia**: `max_active_runs=1`, pool de Airflow `datalake_writer` con 1 slot para
`silver_publish`, `gold_build` y `glue_update_catalog`, y un lock en S3 con escritura condicional
(`If-None-Match`) en `control/locks/datalake-writer.lock`. Además, `silver_publish` verifica que el
commit vigente sea el mismo sobre el que se preparó el trabajo (si no, falla con `StaleWorkError`).

## Incrementalidad
- **Bronze → Silver**: solo lotes sin marcador.
- **Particiones**: se calcula una huella por fecha; solo se reescriben las fechas cuya huella cambió.
- **Comprehend**: solo textos cuyo hash no está en la caché.
- **Gold**: solo las fechas cuya versión Silver cambió (`built_from` en `control/gold/current.json`).
- **Glue**: solo se crean o actualizan las particiones que cambiaron.

## Fallos y recuperación
| Fallo | Qué pasa | Recuperación |
|---|---|---|
| Una fuente responde 5xx / timeout | Reintentos internos (3) + reintentos de Airflow (2). Lote `HTTP_ERROR`/`NETWORK_ERROR` | Las demás fuentes se publican; la ejecución queda `PARTIAL` (en rojo). La fuente no se toma como "sin noticias" |
| Error en Comprehend | Esas noticias quedan `ERROR` | No se cachea; se reintenta en la próxima ejecución |
| Caída antes del commit (fallo controlado) | Nada visible cambia | *Clear* de la tarea en Airflow: reutiliza el trabajo preparado y publica una sola vez |
| Caída entre commit y outbox / Glue | Commit hecho, catálogo atrasado | La siguiente tarea completa el outbox y sincroniza Glue |
| Archivo Bronze alterado | `IntegrityError` por hash distinto | Se detecta antes de procesar |
| Dos escrituras a la vez | Lock / pool | La segunda espera o reintenta |

## Nuevas particiones y metadatos
- Tablas `silver_news*` y `gold_*`: las particiones se registran explícitamente en el paso
  `glue_update_catalog` del mismo DAG, justo después de publicar. No hace falta `MSCK REPAIR` ni crawler.
- `silver_news_history` y `silver_nlp_analysis`: **proyección de particiones** de Athena (rango de
  fechas en las propiedades de la tabla); una fecha nueva es consultable sin registrar nada.
- Se eligieron definiciones explícitas en Terraform (generadas desde `schemas.py`) en lugar de un
  Glue Crawler: el esquema lo controla el código, no hay que esperar ni pagar al crawler, y no se
  arriesga a que infiera tipos distintos. Un crawler tampoco reemplazaría las validaciones de calidad.
