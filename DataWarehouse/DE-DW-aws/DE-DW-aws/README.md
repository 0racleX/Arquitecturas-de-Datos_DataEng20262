# Data warehouse de comercio electrónico colombiano en AWS

Migración del proyecto local [DE-DW](https://github.com/smm-0216/DE-DW) a AWS, conservando sus reglas de
negocio: **RDS for MySQL** y **DocumentDB** como fuentes, **S3** como zona raw por lote, **Redshift
Serverless** como warehouse, **Airflow + dbt** en una **EC2** y un dashboard en **QuickSight**. Toda la
infraestructura se crea y se elimina con **Terraform**.

```bash
./scripts/aws/deploy.sh      # crea todo (~35-45 min) y deja las fuentes cargadas
# ... Airflow: Trigger DAG w/ config -> batch_id=initial_001, mode=full
./scripts/aws/destroy.sh     # elimina todo
```

## Documentación

| Documento | Contenido |
| --- | --- |
| [docs/aws/GUIA.md](docs/aws/GUIA.md) | **Paso a paso**: requisitos en WSL, despliegue, escenarios, QuickSight, capturas 📸 y eliminación |
| [docs/aws/arquitectura.md](docs/aws/arquitectura.md) | Diagrama de arquitectura, DAG y capas en Redshift |
| [docs/aws/decisiones.md](docs/aws/decisiones.md) | Diferencias con la versión local, ajustes dbt/DocumentDB, formato S3, lotes y marca de avance, SPICE, seguridad y costos |
| [docs/aws/quicksight_validacion.sql](docs/aws/quicksight_validacion.sql) | Consulta que valida el indicador del dashboard |
| [docs/aws/consultas_evidencia.sql](docs/aws/consultas_evidencia.sql) | Consultas de evidencia para Query editor v2 |
| [.env.example](.env.example) | Variables requeridas (sin valores secretos) |
| [infra/terraform/terraform.tfvars.example](infra/terraform/terraform.tfvars.example) | Parámetros del despliegue |
| [docs/data_model.md](docs/data_model.md) | Modelo dimensional (sin cambios respecto al proyecto local) |

Las guías de `docs/etapas/`, `docs/architecture.md`, `docs/airflow.md` y `docs/connections.md` describen la versión local original (DuckDB) y se
conservan como referencia del modelo y de las reglas de calidad.

## Estructura

```
infra/terraform/          VPC, S3, IAM, Secrets Manager, RDS, DocumentDB, Redshift Serverless, EC2, QuickSight
docker/                   Imagen Airflow 2.11 + dbt-redshift y docker-compose que corre en la EC2
src/airflow/dags/         DAG ecommerce_pipeline
src/ecommerce_dw/
  ingestion/              Extracción incremental a S3, COPY a Redshift y control de lotes (ops)
  sources/                Generación de datos, lotes diarios y cambios SCD2 (RDS + DocumentDB)
  transformations/        Ejecución de dbt desde Airflow
  ops/evidence.py         Consultas de evidencia
src/dbt/                  Proyecto dbt adaptado a Redshift
scripts/                  Scripts que corren EN LA EC2 (run_pipeline, append_batch, warehouse_report, ...)
scripts/aws/              Scripts que corren EN TU WSL (deploy, ssh, remote, sync_code, destroy)
```
