# Decisiones técnicas, diferencias con la versión local, costos y seguridad

## Diferencias frente a la versión local

| Aspecto | Local (DE-DW original) | AWS (esta versión) |
| --- | --- | --- |
| Fuente relacional | MySQL 8.4 en Docker | Amazon RDS for MySQL 8.4 (subred privada, cifrado) |
| Fuente documental | MongoDB 8.0 en Docker | Amazon DocumentDB 5.0 (TLS obligatorio) |
| Zona raw | CSV temporales por corrida en `warehouse/extracts/` | S3 privado y versionado: `raw/<fuente>/<entidad>/batch_id=<id>/` + manifiesto |
| Warehouse | DuckDB (un archivo, un escritor) | Amazon Redshift Serverless (8 RPU, tope diario) |
| Carga raw | `INSERT ... FROM read_csv` reemplazando la tabla | `COPY` desde S3 con rol IAM; raw acumula todos los lotes |
| Orquestación | 3 DAGs (maestro + ingesta + transformación) | 1 DAG con TaskGroups y parámetros (`batch_id`, `mode`, `fail_task`) |
| Concurrencia | `fcntl` lock sobre el archivo DuckDB, `--threads 1` | Sin lock (Redshift admite concurrencia), dbt con 4 hilos |
| Extracción | Completa en cada corrida | Inicial (`full`) e incremental por marca de agua |
| Credenciales | `.env` con valores locales en el repositorio | Generadas por Terraform, en Secrets Manager; `.env` se crea en la EC2 |
| Control de lotes | No existía | `ops.batch_control`, `ops.extraction_log`, resúmenes JSON en `s3://.../control/` |

## Ajustes de dbt para Redshift

| DuckDB | Redshift | Dónde |
| --- | --- | --- |
| `select * exclude (col)` | Lista explícita de columnas | `int_*__valid.sql` |
| `a is distinct from b` | Macro `is_distinct_from` (comparación null-safe) | `macros/redshift_compat.sql`, `incremental.sql`, pruebas |
| `timezone('America/Bogota', ts)` sobre `TIMESTAMPTZ` | `convert_timezone('UTC', 'America/Bogota', ts)` sobre `TIMESTAMP` UTC | macro `to_business_time` |
| `extract(isodow ...)` | `extract(dow ...)` convertido a ISO (domingo 0 → 7) | `dim_date.sql` |
| `json_extract_string(doc, '$.a.b')` | `json_extract_path_text(doc, 'a', 'b', true)` | `stg_mongodb__reviews.sql` |
| `max(date) - 7` | `dateadd(day, -7, max(date))` | `incremental.sql` |
| `datediff('day', a, b)` | `datediff(day, a, b)` | prueba de `dim_date` |
| Alias de tabla `date` | Alias `dates` (evita ambigüedad con el tipo) | hechos |
| Esquema `raw` | `raw_data` (`RAW` es palabra reservada en Redshift) | `sources.yml`, ingesta |
| `JSON` / `TIMESTAMPTZ` en raw | `VARCHAR(65535)` / `TIMESTAMP` (UTC) | DDL de raw |
| Vistas normales | Vistas *late-binding* (`bind: false`) para que las tablas se puedan reconstruir | `dbt_project.yml` |
| Perfil `duckdb` | Perfil `redshift` con `env_var` (sin secretos en el repositorio) | `profiles.yml` |

Se conservan sin cambios: reglas de calidad y su prioridad, capas `intermediate` / `quarantine` /
`marts`, modelo dimensional (constelación de 3 hechos y dimensiones conformadas), snapshots SCD2 por
`updated_at`, hechos incrementales con `delete+insert`, ventana de reproceso de 7 días, borrado de
hechos huérfanos y todas las pruebas. Se agregaron: `_batch_id` / `_first_batch_id` en staging,
intermediate y quarantine (lote de origen de cada rechazo), y la vista `marts.rpt_sales_dashboard`.

## MongoDB → DocumentDB

| Diferencia | Ajuste |
| --- | --- |
| TLS obligatorio con certificado de AWS | La imagen descarga `global-bundle.pem`; la URI usa `tls=true&tlsCAFile=...` |
| No soporta *retryable writes* | `retryWrites=false` en la URI |
| Réplica `rs0` y lecturas | `replicaSet=rs0&readPreference=secondaryPreferred` |
| `$jsonSchema` con soporte parcial | `ensure_mongo_collections` intenta aplicar el validador; si DocumentDB lo rechaza, crea la colección sin él y lo registra. La calidad la garantiza dbt de todas formas |
| `bypassDocumentValidation` | `insert_unvalidated` lo intenta; si falla, apaga temporalmente la validación (`collMod validationLevel=off`) para inyectar los registros defectuosos |
| Solo accesible dentro de la VPC | El generador y la extracción corren en la EC2 (misma VPC) |
| Extracción incremental | Índices en `review_id` y `event_id` para filtrar por marca de agua |

## Formato en S3: CSV comprimido con gzip

- `COPY ... FORMAT AS CSV GZIP` lo carga de forma nativa, sin librerías adicionales (Parquet requeriría `pyarrow` y un esquema estricto).
- Es legible para auditar un lote (se descarga y abre directo).
- Conserva el documento completo de DocumentDB en `document_json`, con comillas escapadas según RFC 4180.
- `EMPTYASNULL` mantiene la semántica de nulos del proyecto local (`nullstr=''`).
- Contra: no tiene tipos; los tipos los fija el DDL de `raw_data` y se verifican con los conteos.

## Identificación de lotes y marca de avance

- **Identificador**: el parámetro `batch_id` de la corrida o, si está vacío, la fecha lógica en
  formato `YYYYMMDDTHHMMSSZ`. Todas las tareas lo recalculan del contexto, así que los reintentos
  trabajan siempre sobre el mismo lote.
- **Archivos**: `raw/<fuente>/<entidad>/batch_id=<id>/` con datos y `_manifest.json` (filas, fecha de
  extracción, columna y valores de marca de agua). El manifiesto se escribe al final con escritura
  condicional `If-None-Match: *`: si ya existe, la extracción **reutiliza** los archivos y no consulta
  la fuente; nunca sobrescribe otro lote (además, el bucket está versionado).
- **Columnas de linaje**: cada fila raw tiene `_batch_id`, `_extracted_at`, `_source`, `_loaded_at`.
- **Estados en `ops.batch_control`**:

  | Momento | Estado |
  | --- | --- |
  | `validate_connections_and_params` termina | `running` |
  | `load_redshift_raw` termina | `raw_loaded` |
  | `dbt_run_marts` termina | `transformed` |
  | `register_result` termina | `success` (+ `finished_at` y resumen en `s3://.../control/batches/batch_id=<id>/`) |
  | Una tarea agota sus reintentos | `failed` (+ `failed_task` y `message`) |

- **Marca de agua (avance)**: por entidad, el máximo `watermark_to` de `ops.extraction_log` **entre lotes
  en `success`**. Por eso avanza solo cuando `register_result` cierra el lote; si un lote falla, el
  siguiente vuelve a leer ese rango y la deduplicación de staging evita duplicados.

  | Entidad | Columna | Comparación |
  | --- | --- | --- |
  | customers, products | `updated_at` | `>=` (no pierde filas con la misma marca) |
  | orders, order_items | `order_id`, `order_item_id` | `>` |
  | reviews, navigation_events | `review_id`, `event_id` | `>` |

- **Idempotencia**: la carga hace `DELETE ... WHERE _batch_id = <id>` + `COPY` en una transacción;
  staging toma la versión más reciente por llave (`row_number()` por `_extracted_at`); los hechos son
  `delete+insert` por llave subrogada y los snapshots solo abren versión si `updated_at` avanza.
- **Recuperación**: reejecutar el mismo `batch_id`. Lo extraído se reutiliza desde S3, raw se recarga
  de forma idempotente y dbt continúa.
- **Limitaciones conocidas**: la extracción por marca de agua no detecta borrados físicos en el origen
  ni cambios en pedidos existentes (no tienen `updated_at`); una carga `full` vuelve a traer todo.

## SPICE vs. consulta directa

El dataset usa **DIRECT_QUERY**: cada visual consulta `marts.rpt_sales_dashboard` en Redshift, así que
el dashboard refleja el último lote sin pasos adicionales. Si se cambia a SPICE (`import_mode = "SPICE"`),
hay que refrescar después de cada lote, por ejemplo agregando al final del pipeline:

```bash
aws quicksight create-ingestion --aws-account-id <ACCOUNT_ID> --data-set-id de-dw-sales \
  --ingestion-id "batch-<batch_id>" --region us-east-1
```

o programando un *refresh schedule* en el dataset. La columna `model_refreshed_at` permite comprobar
que el dashboard y la consulta de validación usan la misma actualización.

## Seguridad implementada

- Credenciales generadas por Terraform y guardadas solo en **Secrets Manager**; el repositorio no tiene secretos (`.env.example` solo documenta nombres). En la EC2, `.env` tiene permisos `600`.
- **RDS y DocumentDB en subredes privadas** sin ruta a internet; sus security groups solo aceptan la EC2.
- **Redshift** solo acepta la EC2 y el rango de IP publicado de QuickSight para la región.
- **SSH y Airflow** solo desde tu IP pública (`/32`); Airflow con usuario y contraseña aleatoria; llave SSH ED25519 generada (no se reutilizan llaves).
- **IAM de mínimo privilegio**: la EC2 solo lee/escribe su bucket y lee su secreto; Redshift solo lee `raw/*` del bucket.
- **Cifrado**: S3 (SSE-S3), RDS, DocumentDB y EBS cifrados en reposo; TLS en tránsito a DocumentDB y Redshift; la política del bucket rechaza conexiones sin TLS.
- **S3**: bloqueo de acceso público, *object ownership* forzado, versionado y escritura condicional de manifiestos.
- **EC2**: IMDSv2 obligatorio.
- **Costos**: tope diario de RPU-hora en Redshift Serverless.

Simplificaciones aceptadas para el laboratorio (a mejorar en producción): usuarios administradores de
RDS/Redshift para el pipeline (en producción: usuarios dedicados con permisos por esquema), Airflow por
HTTP sin TLS (mitigado restringiendo la IP o con el túnel SSH), estado de Terraform local (contiene las
contraseñas: no lo subas al repositorio; en producción, backend S3 cifrado).

## Costos

Estimación on-demand en us-east-1 (precios de lista; verifica en la calculadora de AWS):

| Recurso | Tamaño | Costo aprox. |
| --- | --- | --- |
| EC2 (Airflow + dbt) | t3.large + 30 GB gp3 + IPv4 pública | ~USD 0.09/h → ~2.2/día |
| RDS for MySQL | db.t4g.micro + 20 GB gp3 | ~USD 0.017/h → ~0.4/día |
| DocumentDB | db.t3.medium + almacenamiento/IO mínimos | ~USD 0.08/h → ~1.9/día |
| Redshift Serverless | 8 RPU base, USD 0.375 por RPU-hora, cobro por segundo con mínimo de 60 s | ~USD 0.3-1 por corrida del pipeline; 0 en reposo |
| S3, Secrets Manager | pocos MB, 1 secreto | < USD 0.05/día |
| QuickSight | 1 autor (prueba gratuita de 30 días) | 0 en la prueba; luego ~USD 24/mes |
| **Total** | encendido todo el día, ~10 corridas | **~USD 5-8/día** |

Controles: tope diario de RPU-hora (`redshift_daily_rpu_hours_limit`, acción `deactivate`), DAG sin
programación automática (solo manual), instancias pequeñas y `scripts/aws/destroy.sh` para eliminar todo.
Las cuentas nuevas pueden tener créditos de prueba de Redshift Serverless y DocumentDB.
