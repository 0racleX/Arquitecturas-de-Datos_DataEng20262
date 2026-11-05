# Guía de despliegue, ejecución, evidencias y eliminación

Todo se despliega con Terraform desde tu Ubuntu en WSL. Las cajas **📸 CAPTURA** marcan dónde tomar
cada captura para la entrega; guárdalas en `evidencias/` (la carpeta está en `.gitignore`).

> Tiempo total aproximado: 35-45 min de despliegue + 30-40 min de escenarios.
> Costo aproximado mientras está encendido: ~USD 5/día (ver [decisiones.md](decisiones.md#costos)). **Destruye todo al terminar.**

---

## 0. Requisitos en WSL (una sola vez)

```bash
# Terraform (repositorio oficial de HashiCorp)
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl unzip jq
wget -O- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform

terraform version
aws sts get-caller-identity   # debe mostrar tu cuenta: el usuario de la CLI ya está configurado
```

Trabaja **dentro del sistema de archivos de WSL** (por ejemplo `~/DE-DW-aws`), no en `/mnt/c/...`:
la llave SSH que genera Terraform necesita permisos `600`, que NTFS no respeta.

```bash
cp -r /mnt/c/RUTA/DONDE/QUEDO/DE-DW-aws ~/DE-DW-aws    # o git clone de tu repositorio
cd ~/DE-DW-aws
find scripts -name '*.sh' -exec chmod +x {} +
```

## 1. Desplegar la infraestructura

```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars   # revisa región y tamaños
./scripts/aws/deploy.sh
```

`deploy.sh` ejecuta `terraform init` y `terraform apply` (escribe `yes` cuando lo pida) y luego espera
a que la EC2 termine su arranque. Terraform crea: VPC, subredes, security groups, bucket S3 privado,
roles IAM, Secrets Manager, RDS MySQL, DocumentDB, Redshift Serverless (con tope diario de RPU) y la
EC2. Al arrancar, la EC2 instala Docker, descarga el código desde S3, genera `.env` desde Secrets
Manager, construye la imagen de Airflow + dbt-redshift, crea las tablas y colecciones y **carga los
datos sintéticos** (con registros defectuosos inyectados) en RDS y DocumentDB.

> 📸 **CAPTURA 1 — Despliegue reproducible**: la salida final de `terraform apply`
> (`Apply complete! Resources: N added` y los `Outputs`).

> 📸 **CAPTURA 2 — Recursos creados** (consola de AWS, opcional pero recomendable): RDS → Databases
> (`de-dw-mysql` *Available*), DocumentDB → Clusters (`de-dw-docdb`), Redshift Serverless →
> Workgroups (`de-dw-wg` *Available*), S3 → bucket `de-dw-lake-...`, EC2 → `de-dw-airflow` *Running*.

Datos que vas a necesitar:

```bash
terraform -chdir=infra/terraform output airflow_url
terraform -chdir=infra/terraform output -raw airflow_admin_password; echo
terraform -chdir=infra/terraform output redshift_endpoint
terraform -chdir=infra/terraform output -raw redshift_admin_password; echo
terraform -chdir=infra/terraform output lake_bucket
```

Si el arranque tarda, síguelo con: `./scripts/aws/ssh.sh sudo tail -f /var/log/de-dw-bootstrap.log`

## 2. Escenario: carga inicial (+ registros defectuosos)

1. Abre la URL de Airflow (usuario `airflow`, contraseña del output). Solo tu IP puede entrar;
   si tu IP cambió, vuelve a correr `terraform -chdir=infra/terraform apply` o usa `./scripts/aws/tunnel_airflow.sh`.
2. DAG `ecommerce_pipeline` → botón ▶ **Trigger DAG w/ config** →
   `batch_id = initial_001`, `mode = full`, `fail_task` vacío → **Trigger**.

> 📸 **CAPTURA 3 — DAG**: vista *Graph* del DAG (validación → extracciones en paralelo → carga → dbt → registro).

> 📸 **CAPTURA 4 — Ejecución exitosa**: vista *Grid* con todas las tareas en verde para `initial_001`.

> 📸 **CAPTURA 5 — Logs con lote y tarea**: log de `extract_to_s3.rds_mysql` mostrando líneas
> `[batch=initial_001] extracted ... rows ... to s3://...`.

> 📸 **CAPTURA 6 — Pruebas dbt**: log de `dbt.dbt_test` con el resumen `Done. PASS=... WARN=0 ERROR=0`.

> 📸 **CAPTURA 7 — S3**: consola S3 → `raw/mysql/customers/batch_id=initial_001/` con
> `customers.csv.gz` y `_manifest.json` (abre el manifiesto: filas, fecha de extracción, marcas de agua).

Evidencias desde Redshift (en tu WSL):

```bash
./scripts/aws/remote.sh warehouse_report.sh batches      # ops.batch_control y ops.extraction_log
./scripts/aws/remote.sh warehouse_report.sh raw          # filas por tabla y lote en raw_data
./scripts/aws/remote.sh warehouse_report.sh quarantine   # rechazos con motivo y lote de origen
./scripts/aws/remote.sh warehouse_report.sh facts
./scripts/aws/remote.sh warehouse_report.sh reconcile    # indicador conciliado origen vs. modelo
```

> 📸 **CAPTURA 8 — Control de lotes**: salida de `batches` (estado `success`, filas extraídas = cargadas).

> 📸 **CAPTURA 9 — Registros defectuosos**: salida de `quarantine` (cada motivo con su `_batch_id`).

> 📸 **CAPTURA 10 — Datos en modelos analíticos y conciliación**: salidas de `facts` y `reconcile` (`coincide = true`).

Las mismas consultas se pueden ejecutar en **Redshift → Query editor v2** (conexión al workgroup
`de-dw-wg` con usuario `dwadmin` y la contraseña del output). Están en [consultas_evidencia.sql](consultas_evidencia.sql).

## 3. Escenario: repetición de un lote exitoso (idempotencia)

```bash
./scripts/aws/remote.sh warehouse_report.sh state > evidencias/antes_repeticion.txt
./scripts/aws/remote.sh run_pipeline.sh --batch-id initial_001 --mode full
./scripts/aws/remote.sh warehouse_report.sh state > evidencias/despues_repeticion.txt
diff evidencias/antes_repeticion.txt evidencias/despues_repeticion.txt && echo "SIN CAMBIOS"
```

La segunda corrida encuentra el manifiesto en S3, **no consulta las fuentes** (log: `already extracted ...
reusing S3 files`), borra y recarga las filas de ese lote en raw y dbt no duplica hechos ni versiones.

> 📸 **CAPTURA 11 — Idempotencia**: log de `extract_to_s3.rds_mysql` con `reusing S3 files` y el `diff` sin diferencias.

## 4. Escenario: nuevo lote incremental (+ repetición del lote)

```bash
./scripts/aws/remote.sh verify_incremental_load.sh --batch-id 1
```

El script agrega a las fuentes el lote diario 1 (3 pedidos, 3 reseñas, 3 eventos), ejecuta el
pipeline con `batch_id=incremental_1` (extracción **incremental** desde la marca de agua), comprueba
que cada hecho creció exactamente 3 filas y concilia, lo reejecuta (sin cambios) y reenvía el mismo
lote de origen con una extracción nueva (sin cambios).

> 📸 **CAPTURA 12 — Incremental**: salida completa del script hasta `Incremental load verified`.

> 📸 **CAPTURA 13 — Marca de agua**: `./scripts/aws/remote.sh warehouse_report.sh batches`, donde
> `incremental_1` muestra `watermark_from` = `watermark_to` del lote anterior y pocas filas extraídas.

## 5. Escenario: cambio en un atributo historizado (SCD tipo 2)

```bash
./scripts/aws/remote.sh warehouse_report.sh scd2                  # antes: una versión
./scripts/aws/remote.sh simulate_changes.sh                       # cliente 1 -> Medellín; producto 1 -> Hogar (vigente 2025-07-01)
./scripts/aws/remote.sh run_pipeline.sh --batch-id scd2_001
./scripts/aws/remote.sh warehouse_report.sh scd2                  # después: dos versiones
```

> 📸 **CAPTURA 14 — SCD2**: salida de `scd2` antes y después: la versión anterior queda con
> `valid_to = 2025-07-01` e `is_current = false` y la nueva con `is_current = true`.

Para un segundo cambio usa otra fecha: `simulate_changes.sh --city Cali --department "Valle del Cauca" --changed-at 2025-10-01`.

## 6. Escenario: fallo controlado y reejecución

```bash
./scripts/aws/remote.sh append_batch.sh --batch-id 2                              # 3 filas nuevas por hecho
./scripts/aws/remote.sh warehouse_report.sh facts
./scripts/aws/remote.sh run_pipeline.sh --batch-id fail_001 --fail-task load_redshift_raw   # falla a propósito
./scripts/aws/remote.sh warehouse_report.sh batches                               # fail_001 = failed, failed_task
./scripts/aws/remote.sh run_pipeline.sh --batch-id fail_001                       # reejecución del MISMO lote
./scripts/aws/remote.sh warehouse_report.sh batches
./scripts/aws/remote.sh warehouse_report.sh facts                                 # +3 filas exactas, sin duplicados
./scripts/aws/remote.sh warehouse_report.sh reconcile
```

La tarea `load_redshift_raw` carga raw y luego falla; Airflow la reintenta 2 veces (30 s, 60 s) y al
agotar los reintentos el callback marca el lote `failed` con la tarea y el error. Como el lote no quedó
en `success`, **la marca de agua no avanzó**. La reejecución con el mismo `batch_id` reutiliza los
archivos de S3, vuelve a cargar raw de forma idempotente (DELETE del lote + COPY) y completa dbt.

> 📸 **CAPTURA 15 — Fallo**: vista *Grid* con `load_redshift_raw` en rojo (intentos 1-3) y su log
> `Controlled failure requested for task load_redshift_raw`.

> 📸 **CAPTURA 16 — Registro del fallo**: `batches` con `fail_001 | failed | load_redshift_raw`.

> 📸 **CAPTURA 17 — Recuperación**: `batches` con `fail_001 | success`, `facts` con exactamente 3 filas
> más por hecho y `reconcile` con `coincide = true`.

También se puede reejecutar desde la UI: en la corrida fallida, selecciona la tarea → **Clear** →
*Downstream* (con `fail_task` la tarea fallaría otra vez, por eso el script lanza una corrida nueva sin él).

## 7. QuickSight

1. Consola → **QuickSight** → *Sign up* (misma región que Redshift, p. ej. us-east-1). La prueba
   gratuita de 30 días es suficiente. No hace falta conceder acceso a S3.
2. Obtén el ARN de tu usuario:
   ```bash
   aws quicksight list-users --aws-account-id "$(aws sts get-caller-identity --query Account --output text)" \
     --namespace default --region us-east-1 --query 'UserList[].Arn' --output text
   ```
3. En `infra/terraform/terraform.tfvars`: `enable_quicksight = true` y `quicksight_user_arn = "<ARN>"`, luego:
   ```bash
   terraform -chdir=infra/terraform apply
   ```
   Se crean el data source (Redshift Serverless) y el dataset `DE-DW ventas (marts.rpt_sales_dashboard)`
   en modo **DIRECT_QUERY**. *(Alternativa manual: Datasets → New → Redshift → Manual connect con el endpoint,
   puerto 5439, base `ecommerce_dw`, usuario `dwadmin`, y SQL `select * from marts.rpt_sales_dashboard`.)*
4. Datasets → el dataset → **Use in analysis** y crea:
   - **KPI**: `subtotal` (Sum) → título "Ventas totales".
   - **Gráfica de líneas o barras**: X = `sale_date` (agregado por mes), Value = `subtotal` (Sum).
   - **Barras por categoría**: Y = `category`, Value = `subtotal` (Sum).
   - **Filtro**: Filter → Add → `category` (o `sales_channel`) → *Add to sheet* como control desplegable.
   - Opcional: tabla o KPI con `model_refreshed_at` (Max) para mostrar la fecha de actualización.
5. **Publish** → *Publish dashboard* → nombre "DE-DW Ventas".

> 📸 **CAPTURA 18 — Dashboard**: el dashboard publicado con KPI, gráfica y control de filtro.

> 📸 **CAPTURA 19 — Validación del indicador**: aplica un filtro (p. ej. `category = Electrónica`) y
> captura el KPI; ejecuta la consulta equivalente de [quicksight_validacion.sql](quicksight_validacion.sql)
> en Query editor v2 (o `warehouse_report.sh sql "..."`) y captura el mismo valor y el mismo `model_refreshed_at`.

## 8. Demostración: un lote nuevo de punta a punta

```bash
./scripts/aws/remote.sh append_batch.sh --batch-id 3
./scripts/aws/remote.sh run_pipeline.sh --batch-id demo_003
```

Con DIRECT_QUERY, al recargar el dashboard el KPI ya incluye el lote (sube la suma de 3 líneas del
2026-01-03). Para explicar una ejecución fallida: `ops.batch_control` dice qué lote falló, en qué tarea y
con qué error; los logs de Airflow tienen el prefijo `[batch=<id>]`; la recuperación es reejecutar el mismo
`batch_id` (sección 6).

## 9. Eliminar todos los recursos

```bash
./scripts/aws/destroy.sh            # escribe yes
```

Elimina EC2, RDS, DocumentDB, Redshift Serverless, S3 (incluido su contenido), secretos, IAM y red.
Si activaste QuickSight, borra el dashboard/análisis en la consola y **cancela la suscripción**
(Manage QuickSight → Account settings → Delete account), que Terraform no administra.

> 📸 **CAPTURA 20 — Eliminación**: `Destroy complete! Resources: N destroyed.`

---

## Comandos útiles

| Objetivo | Comando (desde WSL) |
| --- | --- |
| Entrar a la EC2 | `./scripts/aws/ssh.sh` |
| Ejecutar un script del proyecto en la EC2 | `./scripts/aws/remote.sh <script>.sh [args]` |
| Subir cambios de código a la EC2 | `./scripts/aws/sync_code.sh` |
| Consulta SQL libre en Redshift | `./scripts/aws/remote.sh warehouse_report.sh sql "select ..."` |
| dbt completo fuera de Airflow | `./scripts/aws/remote.sh run_dbt.sh` |
| Reiniciar el laboratorio (fuentes + warehouse) | `./scripts/aws/remote.sh reset_environment.sh --purge-s3` |
| Logs de Airflow | `./scripts/aws/ssh.sh 'cd /opt/de-dw && docker compose -f docker/docker-compose.yml logs -f airflow-scheduler'` |

## Problemas frecuentes

- **No abre Airflow**: tu IP cambió → `terraform -chdir=infra/terraform apply` (actualiza el security group) o `./scripts/aws/tunnel_airflow.sh` y abre `http://localhost:8080`.
- **`Permissions 0777 for key are too open`**: estás en `/mnt/c`; mueve el proyecto a `~`.
- **Redshift deja de responder**: se alcanzó el tope diario de RPU (`redshift_daily_rpu_hours_limit`); súbelo o cambia `redshift_usage_breach_action = "log"`.
- **Cuenta de AWS Academy / Learner Lab**: no permite crear roles IAM; esta guía asume una cuenta con permisos completos.
