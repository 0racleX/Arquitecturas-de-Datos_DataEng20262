# 7. Despliegue, ejecución y eliminación

Instrucciones para reproducir el data lake en una cuenta de AWS, ejecutarlo y eliminar los recursos.

## 1. Requisitos

| Herramienta | Versión | Verificación |
|---|---|---|
| Cuenta de AWS | Usuario con permisos sobre S3, EC2, IAM, Glue, Athena, SSM y Comprehend | `aws sts get-caller-identity` |
| AWS CLI | v2 | `aws --version` |
| Terraform | ≥ 1.6 | `terraform version` |
| Session Manager plugin | cualquiera reciente | `session-manager-plugin --version` |
| Python | 3.11 (solo para las pruebas locales) | `python3.11 --version` |

**Región**: todo se despliega en `us-east-1`. La AWS CLI debe usar la misma región; si no, las
llamadas a la instancia responden `TargetNotConnected` o `InvalidInstanceID.NotFound`:

```bash
aws configure set region us-east-1
```

Instalación del Session Manager plugin (necesario para la consola de la EC2 y el túnel a Airflow):

```bash
# Ubuntu / Debian / WSL
curl "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o session-manager-plugin.deb
sudo dpkg -i session-manager-plugin.deb
# macOS
brew install --cask session-manager-plugin
```

En Windows (PowerShell) se instala con
[SessionManagerPluginSetup.exe](https://s3.amazonaws.com/session-manager-downloads/plugin/latest/windows/SessionManagerPluginSetup.exe).
Donde aparezca `$(terraform output -raw X)`, ejecute `terraform output -raw X` y use el valor.

Si la cuenta no tiene VPC por defecto: `aws ec2 create-default-vpc`.

## 2. Pruebas locales (opcional, no usan AWS)

```bash
python3.11 -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
pytest -v                                                  # 36 passed
```

Las pruebas recorren los escenarios de calidad, incrementalidad y recuperación con S3 y Glue
simulados (moto) y un Comprehend falso.

## 3. Despliegue

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars    # ajustar si se desea (región, tipo de instancia, horario)
terraform init
terraform validate
terraform plan -out plan.tfplan
terraform apply plan.tfplan
```

Se crean unos 75 recursos:
- **S3**: un bucket con todas las zonas.
- **Glue**: una base de datos con 13 tablas.
- **Athena**: un workgroup con 20 consultas guardadas.
- **IAM**: un rol para la EC2.
- **EC2** con Airflow.
- **Systems Manager**: un parámetro con la contraseña de Airflow.

Si el `apply` falla con `GetObjectTagging … NoSuchKey` (propagación de un bucket recién creado),
vuelva a ejecutar `terraform apply`.

Variables de trabajo:

```bash
export BUCKET=$(terraform output -raw bucket_name)
export INSTANCE=$(terraform output -raw instance_id)
```

### 3.1 Esperar la instalación de Airflow (~10 min)

La EC2 instala PostgreSQL y Airflow 2.10.5, y luego sincroniza el código desde
`s3://<bucket>/artifacts/` cada 2 minutos.

```bash
aws ssm start-session --target $INSTANCE
sudo tail -f /var/log/user-data.log        # termina con "Bootstrap completo"; salir con Ctrl+C y exit
```

Si `start-session` responde `TargetNotConnected` en los primeros minutos, espere y repita. El avance
también se ve sin SSM:
`aws ec2 get-console-output --instance-id $INSTANCE --latest --output text | tail -30`.

### 3.2 Abrir Airflow

En una terminal aparte, que debe quedar abierta:

```bash
$(terraform output -raw airflow_ui_tunnel_cmd)
```

Abra http://localhost:8080. El usuario es `admin`; la contraseña se obtiene con:

```bash
$(terraform output -raw airflow_admin_password_cmd)
```

### 3.3 Consola de la EC2 (para lotes controlados y CLI)

```bash
aws ssm start-session --target $INSTANCE
sudo -iu airflow
cd /opt/news-datalake
```

En esa consola están disponibles los atajos `py` (Python del pipeline) y `af` (CLI de Airflow), y
la variable `$DATALAKE_BUCKET`.

## 4. Ejecución

### 4.1 DAG `news_datalake_pipeline`

Al activar el interruptor del DAG, Airflow lanza la primera ejecución. Después corre cada hora en
punto (UTC). Para lanzarlo a mano: **▶ Trigger DAG**, elija el modo y pulse **Trigger**.

| mode | reprocess_run_id | Qué hace |
|---|---|---|
| `extract` | `-` | Consulta los RSS, escribe en Bronze y procesa los lotes pendientes hasta Gold y Glue |
| `reprocess` | run_id de un lote Bronze | No consulta los RSS; vuelve a procesar ese lote desde Bronze sin duplicar resultados |
| `rebuild` | `-` | No consulta los RSS; reconstruye Silver y Gold desde todo Bronze (sin nuevas llamadas a Comprehend) |

En `extract` y `rebuild` el formulario exige un valor en `reprocess_run_id`: escriba `-`, que se ignora.

Desde la consola de la EC2 se obtiene lo mismo con:

```bash
af dags trigger news_datalake_pipeline --conf '{"mode": "reprocess", "reprocess_run_id": "<run_id>"}'
```

Los `run_id` de los lotes se consultan en Athena con `17_lotes_bronze`.

### 4.2 Pasos del DAG

`validate_environment` → `extract_source` (una por fuente) → `silver_clean_validate_dedup` →
`silver_enrich_comprehend` → `silver_publish` → `gold_build` → `glue_update_catalog` →
`athena_validate` → `record_status`.

Una ejecución termina en estado:
- **COMPLETE** si todo salió bien.
- **PARTIAL** si falló alguna fuente pero las demás se publicaron.
- **FAILED** si falló un paso o una validación.

El estado queda en la tabla `pipeline_run_log` (consulta `12_estado_ejecuciones`). En `PARTIAL` y
`FAILED` la ejecución queda en rojo en Airflow a propósito, para que el fallo no pase desapercibido.

### 4.3 Lotes controlados (copias de un RSS de Bronze)

Desde la consola de la EC2 (sección 3.3). Cada comando crea un lote Bronze nuevo y no modifica el
original. Después, procese el lote con un Trigger en modo `reprocess` y el `run_id` que imprime.

```bash
# Registros inválidos (sin enlace, fecha inválida, sin título, enlace ftp) + 1 repetido
py -m news_pipeline.scenarios --bucket $DATALAKE_BUCKET --source eltiempo_ciencia --scenario defectuoso
# Cambia el título de una noticia: versión nueva y nuevo análisis de Comprehend solo para ella
py -m news_pipeline.scenarios --bucket $DATALAKE_BUCKET --source eltiempo_ciencia --scenario texto
# Cambia solo la fecha: versión nueva sin llamar a Comprehend
py -m news_pipeline.scenarios --bucket $DATALAKE_BUCKET --source eltiempo_medio_ambiente --scenario fecha
# Mueve una noticia 30 días atrás: partición de fecha nueva registrada en Glue
py -m news_pipeline.scenarios --bucket $DATALAKE_BUCKET --source eltiempo_viajar --scenario nueva_particion
```

La siguiente ejecución en modo `extract` vuelve a leer el RSS real y restaura el contenido
original como una versión más reciente. Es el comportamiento esperado: la versión vigente siempre
es la del lote más reciente.

### 4.4 Fallo controlado y recuperación

1. Airflow → **Admin → Variables → +**: Key `news_simulate_failure`, Val `silver_publish` (o `gold_build`).
2. Lance el DAG. La tarea falla **antes del commit**: Athena sigue mostrando la versión anterior y
   `record_status` registra `FAILED`.
3. Borre la variable.
4. En la vista Grid, haga clic en la tarea fallida → **Clear task** → active **Downstream**
   (*Affected Tasks: 5*) → **Clear**.

La misma ejecución se completa una sola vez, sin duplicados.

### 4.5 Fuente no disponible

En `config/sources.yaml`, cambie `eltiempo_fuente_caida` a `enabled: true` y ejecute `terraform
apply`. Espere 2 minutos a que la EC2 sincronice la configuración y lance el DAG en modo `extract`.
La ejecución queda `PARTIAL`, con la fuente en `HTTP_ERROR` o `MALFORMED_XML`. Al terminar, vuelva a
`enabled: false` y ejecute `terraform apply`.

### 4.6 Verificación en Athena

Consola de Athena → workgroup **`news-datalake`** (selector arriba a la derecha) → **Saved
queries**. Base de datos: `news_datalake`.

| Consulta | Uso |
|---|---|
| `02`, `03`, `04`, `05`, `06` | Indicadores de Gold (las cuatro preguntas del enunciado) |
| `01` | Noticias por fecha y sección |
| `07` | Conciliación Gold vs. Silver (diferencia = 0) |
| `08a` / `08b` | Bytes escaneados con y sin filtro de partición |
| `09`, `10`, `11` | Conteos por lote, cuarentena e historial de cambios |
| `12`, `13`, `14` | Estado de ejecuciones y validaciones |
| `15`, `16` | Resultados y estados de Comprehend |
| `17`, `18`, `19` | Lotes Bronze, particiones y posibles duplicados por título |

### 4.7 Ejecución sin Airflow (respaldo)

Desde la consola de la EC2:

```bash
py -m news_pipeline.cli all --bucket $DATALAKE_BUCKET        # ingest + silver + gold + catalog
```

## 5. Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `TargetNotConnected` / `InvalidInstanceID.NotFound` | La CLI usa otra región | `aws configure set region us-east-1` |
| `SessionManagerPlugin is not found` | Falta el plugin | Instalarlo (sección 1) |
| El DAG no aparece | La EC2 aún no sincroniza el código | Esperar 2 min; en la EC2: `ls /opt/news-datalake/dags` |
| Tareas en `queued` indefinidamente | Falta el pool de escritura | En la EC2: `af pools set datalake_writer 1 "writer"` |
| `athena_validate` falla | Una validación no pasó | Consulta `13_resultados_validaciones` |
| `LockBusyError` | Una ejecución murió con el lock tomado | Se libera solo a los 30 min, o borrar `control/locks/datalake-writer.lock` |

## 6. Eliminación de recursos

**Pausa entre sesiones** (conserva datos, catálogo y consultas; elimina lo que se cobra por hora):

```bash
cd terraform
terraform destroy -target=aws_instance.airflow
```

Para volver: `terraform apply`. Se crea una EC2 nueva, que instala Airflow en ~10 min y continúa de
forma incremental con los datos de S3. Cambia el `instance_id`; la contraseña se conserva.

**Eliminación total**:

1. Consola de S3 → bucket `news-datalake-<cuenta>-us-east-1` → **Empty** → escribir
   `permanently delete`. El bucket tiene versionado y `force_destroy = false`, así que Terraform no
   lo borra con datos.
2. Ejecutar:
   ```bash
   cd terraform
   terraform destroy
   ```

El estado de Terraform (`terraform.tfstate`) queda en el equipo local y contiene la contraseña
generada de Airflow. No debe subirse al repositorio (está en `.gitignore`).