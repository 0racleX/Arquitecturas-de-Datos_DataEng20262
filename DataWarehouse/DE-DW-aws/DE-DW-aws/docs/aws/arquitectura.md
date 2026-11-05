# Arquitectura en AWS

```mermaid
flowchart LR
    subgraph VPC["VPC 10.40.0.0/16 (Terraform)"]
        subgraph Privadas["Subredes privadas (sin internet)"]
            RDS[(Amazon RDS for MySQL 8.4<br/>clientes, productos, pedidos, líneas)]
            DDB[(Amazon DocumentDB 5.0<br/>reseñas, eventos de navegación)]
        end
        subgraph Publicas["Subredes públicas"]
            EC2["EC2 t3.large (Docker)<br/>Airflow 2.11 + dbt-redshift 1.9<br/>PostgreSQL de metadatos"]
            RS[(Amazon Redshift Serverless<br/>raw_data · intermediate · quarantine<br/>snapshots · marts · ops)]
        end
        S3EP{{Endpoint gateway S3}}
    end
    S3[(Amazon S3 privado<br/>raw/&lt;fuente&gt;/&lt;entidad&gt;/batch_id=&lt;id&gt;/<br/>control/batches/...)]
    SM[[Secrets Manager<br/>de-dw/connections]]
    QS[Amazon QuickSight<br/>dashboard de ventas]
    You((Tu WSL<br/>Terraform + SSH))

    You -- "terraform apply" --> VPC
    You -- "SSH / :8080 solo tu IP" --> EC2
    SM -- "rol IAM de la EC2" --> EC2
    EC2 -- "1. extracción (TLS)" --> RDS
    EC2 -- "1. extracción (TLS)" --> DDB
    EC2 -- "2. CSV.gz + manifiesto" --> S3EP --> S3
    EC2 -- "3. COPY + dbt (SQL)" --> RS
    RS -- "COPY con rol IAM" --> S3
    QS -- "DIRECT_QUERY a marts.rpt_sales_dashboard<br/>(rango IP de QuickSight)" --> RS
```

## Flujo del DAG `ecommerce_pipeline`

```mermaid
flowchart LR
    V[validate_connections_and_params] --> M[extract_to_s3.rds_mysql]
    V --> D[extract_to_s3.documentdb]
    M --> L[load_redshift_raw]
    D --> L
    L --> C[verify_raw_counts]
    C --> deps[dbt.dbt_deps] --> seed[dbt.dbt_seed] --> int[dbt.dbt_run_intermediate]
    int --> q[dbt.dbt_run_quarantine] --> snap[dbt.dbt_snapshot] --> marts[dbt.dbt_run_marts]
    marts --> tr[dbt.mark_transformed] --> test[dbt.dbt_test] --> R[register_result]
```

| Paso del enunciado | Tarea |
| --- | --- |
| 1. Validación de conexiones y parámetros | `validate_connections_and_params` (MySQL, DocumentDB, S3, Redshift, batch_id, mode) |
| 2. Extracción de las fuentes | `extract_to_s3.rds_mysql`, `extract_to_s3.documentdb` (en paralelo) |
| 3. Escritura en S3 | dentro de cada extracción: `<entidad>.csv.gz` + `_manifest.json` |
| 4. Carga a Redshift | `load_redshift_raw` (COPY con rol IAM) y `verify_raw_counts` |
| 5. Transformaciones y snapshots dbt | grupo `dbt` |
| 6. Pruebas de calidad | `dbt.dbt_test` |
| 7. Registro del resultado | `register_result` (+ callback de fallo en todas las tareas) |

## Capas en Redshift

| Esquema | Contenido | Materialización |
| --- | --- | --- |
| `raw_data` | Copia de cada lote extraído, con `_batch_id`, `_extracted_at`, `_source`, `_loaded_at` | tablas (append por lote) |
| `intermediate` | `stg_*` (versión más reciente por llave), `int_*__qa` (con `rejection_reason`), `int_*__valid`, seeds | vistas late-binding |
| `quarantine` | `quarantine_*`: filas rechazadas con `rejection_reason` y `_batch_id` | tablas |
| `snapshots` | `snap_dim_customer`, `snap_dim_product` (SCD tipo 2) | snapshots dbt |
| `marts` | dimensiones, `fct_sales`, `fct_reviews`, `fct_navigation_events` (incrementales) y la vista `rpt_sales_dashboard` | tablas / incremental / vista |
| `ops` | `batch_control` (estado del lote) y `extraction_log` (filas y marcas de agua por entidad) | tablas |
