# 2. Diccionario de datos (Silver y Gold)

Todas las tablas viven en la base de Glue `news_datalake`. Formato Parquet (Snappy) salvo las
tablas de control, que son JSON. Las fechas `*_utc` son `timestamp` en UTC.

**Partición `pub_date`** (texto `AAAA-MM-DD`): fecha de publicación de la noticia **en UTC**. Se
eligió porque todas las preguntas analíticas son por día de publicación (noticias por día,
presencia de una entidad entre fechas). Ojo: una nota publicada a las 21:00 en Colombia (UTC-5)
cae en el día siguiente en UTC.

## Silver

### `silver_news` — una fila por noticia vigente
Partición: `pub_date`. Clave: `news_id` (única en el conjunto vigente).

| Columna | Tipo | Descripción |
|---|---|---|
| news_id | string | Identificador estable (sha256 de la clave de identidad, 32 caracteres). Ver reglas de identificación |
| id_strategy | string | Cómo se obtuvo el id: `guid_url`, `guid`, `link`, `title_date_hash` |
| identity_key | string | Clave legible usada para el id (URL canónica en El Tiempo) |
| guid | string | `guid` original del RSS |
| link / link_canonical | string | Enlace original / URL canónica (https, sin `utm_*`, sin `/` final) |
| title / title_raw | string | Título limpio (sin HTML, espacios normalizados) / original |
| description / description_raw | string | Descripción limpia / original con HTML |
| pub_date_utc / pub_date_raw | timestamp / string | Fecha de publicación normalizada a UTC / texto original del RSS |
| title_fingerprint | string | Hash del título sin tildes ni mayúsculas (detectar la misma nota con otra URL) |
| medio | string | Medio (`El Tiempo`) |
| first_source_id / first_seccion | string | Primer feed en el que apareció la noticia |
| source_ids / secciones | array&lt;string&gt; | Todos los feeds / secciones donde ha aparecido |
| n_sources | int | Número de feeds donde apareció |
| content_hash | string | Huella de título + descripción + URL canónica + fecha. Si cambia, hay versión nueva |
| version | int | Versión vigente (1 = original) |
| first_seen_at_utc | timestamp | Cuándo se ingirió por primera vez |
| last_change_at_utc | timestamp | Ingesta del lote que produjo la versión vigente |
| first_bronze_run_id / first_bronze_uri | string | Lote y archivo Bronze donde apareció por primera vez |
| bronze_run_id / bronze_uri / bronze_sha256 | string | Lote, archivo y hash Bronze de la versión vigente |
| text_hash | string | sha256 del texto analizado (título + descripción) |
| nlp_status | string | `ANALYZED`, `ANALYZED_TRUNCATED`, `SKIPPED_EMPTY`, `SKIPPED_UNSUPPORTED_LANGUAGE`, `PENDING_QUOTA`, `ERROR` |
| nlp_language | string | Idioma usado en Comprehend |
| nlp_analyzed_at_utc | timestamp | Fecha del análisis |
| silver_version / silver_published_at_utc | string / timestamp | Ejecución que publicó la partición y cuándo |

### `silver_news_entities` — una fila por entidad distinta en cada noticia vigente
Partición: `pub_date`. Clave: (`news_id`, `entity_type`, `entity_text_norm`). Solo tipos PERSON, ORGANIZATION, LOCATION.

| Columna | Tipo | Descripción |
|---|---|---|
| news_id | string | Noticia a la que pertenece (siempre existe en `silver_news`) |
| entity_type / entity_category | string | `PERSON`/`persona`, `ORGANIZATION`/`organizacion`, `LOCATION`/`lugar` |
| entity_text | string | Forma más frecuente en el texto |
| entity_text_norm | string | Minúsculas sin tildes: define "entidad distinta" |
| mention_count | int | Veces que aparece en el texto de la noticia |
| max_score / avg_score | double | Confianza devuelta por Comprehend |
| text_hash, language, analyzed_at_utc | | Trazabilidad del análisis |
| pub_date_utc, medio, first_source_id, secciones | | Copia de la noticia, para filtrar sin JOIN |
| silver_version | string | Ejecución que publicó la partición |

### `silver_news_key_phrases` — una fila por frase clave distinta en cada noticia vigente
Igual que la anterior con `key_phrase` / `key_phrase_norm` en vez de la entidad.

### `silver_news_history` — versiones de cada noticia (opcional, implementado)
Partición: `processed_date` (proyección de particiones). Una fila por evento `NEW` o `UPDATED` con
`changed_fields`, `text_changed`, `previous_content_hash`, y el lote Bronze que lo originó. **No se
usa para métricas**: la versión vigente es la de `silver_news`, así que las versiones anteriores no
inflan los indicadores.

### `silver_nlp_analysis` — caché y auditoría de Comprehend
Partición: `analyzed_date` (proyección). Una fila por `text_hash` analizado: `analyzed_text`,
`text_bytes`, `truncated`, `status`, `language`, `language_score`, `language_source`
(`detected`/`default`), `analyzed_at_utc`, `analysis_version` y las listas `entities` y
`key_phrases` con texto, tipo, `score` y posiciones.

### `silver_quarantine` — registros inválidos
Fuente, medio, sección, `bronze_run_id`, `bronze_uri`, posición del ítem en el XML, campos crudos,
`reasons` (lista) y `reason` (texto). Motivos: `MISSING_TITLE`, `MISSING_LINK`, `INVALID_LINK`,
`MISSING_PUB_DATE`, `INVALID_PUB_DATE`, `FUTURE_PUB_DATE`, `IMPLAUSIBLE_PUB_DATE`.

### `silver_batch_log` — un registro por lote Bronze procesado
`items_read`, `invalid`, `duplicates_in_batch`, `new`, `updated`, `unchanged`, `stale`,
`cross_feed_matches`, `reprocessed`, `full_refresh`, `sequence` (commit de Silver).

## Gold

Todas particionadas por `pub_date`, Parquet, recalculadas solo para las fechas que cambiaron.

### `gold_news_daily` — grano: día × medio × sección
| Métrica | Definición |
|---|---|
| news_count | Noticias **distintas** que aparecieron en esa sección ese día. Una nota publicada en dos feeds cuenta en ambas secciones |
| news_count_first_section | Noticias distintas cuyo **primer** feed fue esa sección. Cada noticia cuenta una sola vez: la suma por día es el total de noticias del día. **Use esta para totales** |
| multi_feed_news_count | De `news_count`, cuántas aparecieron en más de un feed |
| analyzed_news_count | De `news_count`, cuántas tienen entidades/frases (Comprehend `ANALYZED*`) |

### `gold_entity_daily` — grano: día × tipo de entidad × entidad normalizada
| Métrica | Definición |
|---|---|
| news_count | Noticias **distintas** que mencionan la entidad ese día. Repetir el nombre dentro de un texto no la infla |
| mention_count | Menciones totales (informativa, sí cuenta repeticiones) |
| avg_score / max_score | Confianza de Comprehend |

Sumar `news_count` entre días es válido: cada noticia tiene una sola fecha de publicación.

### `gold_key_phrase_section_daily` — grano: día × medio × sección × frase normalizada
`news_count`: noticias distintas de esa sección que contienen la frase. `mention_count`: informativa.

### Noticias sin fecha de publicación válida
No llegan a Silver ni a Gold: la fecha es obligatoria porque define la partición y todas las
métricas diarias. Quedan en `silver_quarantine` con `MISSING_PUB_DATE`, `INVALID_PUB_DATE`,
`FUTURE_PUB_DATE` (más de 24 h en el futuro) o `IMPLAUSIBLE_PUB_DATE` (antes del 2000), y se cuentan
en `silver_batch_log.invalid`. Así ningún indicador diario se calcula con una fecha inventada (por
ejemplo, la fecha de ingesta), y siguen siendo trazables hasta su lote Bronze.

## Control (JSON)

| Tabla | Contenido |
|---|---|
| `bronze_ingestion_log` | Manifiesto por lote: `run_id`, fuente, URL, `ingestion_ts_utc`, `http_status`, `content_sha256`, `s3_uri`, estado, error, `mode` (`extract`/`controlled_copy`) |
| `pipeline_run_log` | Estado por ejecución del DAG: `COMPLETE`/`PARTIAL`/`FAILED`, detalle por fuente, tareas fallidas |
| `quality_check_results` | Resultado de cada validación de Athena por ejecución |
