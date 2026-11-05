# 5. Estimación de costos y controles de seguridad

## Supuestos
- Región `us-east-1`. Precios on-demand de lista; verifíquelos en la
  [AWS Pricing Calculator](https://calculator.aws/) antes de entregar.
- Laboratorio: la EC2 encendida **30 horas** en total (p. ej. 6 h × 5 días) y el resto del mes
  eliminada o detenida.
- DAG cada hora mientras la EC2 está encendida: 3 feeds × ~20 ítems; ~30 noticias nuevas por día.
- Texto analizado ≈ 300 caracteres por noticia.

## Costo por servicio

| Servicio | Precio de referencia | Cálculo | Estimado |
|---|---|---|---|
| EC2 t3.medium | ≈ USD 0,0416 / h | 30 h | ≈ USD 1,25 |
| IPv4 pública de la EC2 | USD 0,005 / h | 30 h | ≈ USD 0,15 |
| EBS gp3 30 GB | USD 0,08 / GB-mes | 30 GB solo mientras existe la instancia (~4 % del mes) | ≈ USD 0,10 (USD 2,40 si queda todo el mes detenida) |
| S3 Standard | USD 0,023 / GB-mes | < 100 MB (XML + Parquet + JSON) | < USD 0,01 |
| S3 solicitudes | USD 0,005 / 1.000 PUT | ~200 PUT por ejecución × 30 | ≈ USD 0,03 |
| Comprehend | USD 0,0001 por unidad de 100 caracteres, mínimo 3 unidades por solicitud, por API | 3 APIs × 3 unidades × ~150 noticias | ≈ USD 0,14 (dentro de la capa gratuita de 50.000 unidades por API al mes, si aplica) |
| Athena | USD 5 / TB escaneado, mínimo 10 MB por consulta | ~13 consultas por ejecución × 30 + consultas manuales ≈ 450 × 10 MB ≈ 4,5 GB | ≈ USD 0,02 |
| Glue Data Catalog | Gratis hasta 1 millón de objetos y 1 millón de solicitudes al mes | ~15 tablas, pocos cientos de particiones | USD 0 |
| Systems Manager (Session Manager, Parameter Store estándar) | Sin costo | | USD 0 |
| **Total del laboratorio** | | | **≈ USD 2** |

Si la EC2 se deja encendida todo el mes, el costo sube a ≈ USD 30 (EC2) + USD 3,65 (IPv4) +
USD 2,40 (disco). Por eso la guía indica eliminarla al terminar cada sesión.

**Controles de costo implementados**
- Caché de Comprehend por hash de texto: no se paga dos veces por el mismo texto, ni siquiera en un `rebuild`.
- `max_documents_per_run: 2000` limita las llamadas a Comprehend por ejecución.
- Workgroup de Athena con `bytes_scanned_cutoff_per_query` = 1 GB.
- Particionamiento por fecha: las consultas filtradas por `pub_date` escanean solo esa fecha.
- Expiración automática: `athena-results/` y `work/` (7 días), `quarantine/bronze/` (30 días) y versiones no vigentes de objetos (30 días).

Fuentes de precios: [Amazon Comprehend pricing](https://aws.amazon.com/comprehend/pricing),
[Amazon EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/),
[Amazon Athena pricing](https://aws.amazon.com/athena/pricing/),
[Amazon S3 pricing](https://aws.amazon.com/s3/pricing/),
[AWS Glue pricing](https://aws.amazon.com/glue/pricing/).

## Controles de seguridad

| Control | Implementación |
|---|---|
| Bucket privado | Block Public Access completo y `BucketOwnerEnforced` (sin ACLs) |
| Cifrado en reposo | SSE-S3 (AES-256) con bucket key en el bucket; disco EBS cifrado; resultados de Athena cifrados |
| Cifrado en tránsito | Política del bucket que niega cualquier acceso sin HTTPS (`aws:SecureTransport = false`) |
| Versionado | Activado: recuperación ante sobrescrituras o borrados accidentales |
| Sin puertos abiertos | El security group no tiene reglas de entrada; la UI de Airflow se abre con un túnel de Session Manager |
| Sin llaves de acceso en la EC2 | La instancia usa un rol IAM (instance profile); IMDSv2 obligatorio |
| Mínimo privilegio | El rol solo accede a **ese** bucket, a **esa** base de Glue y a **ese** workgroup de Athena; Comprehend solo con las APIs de detección |
| Sin secretos en el código | La contraseña de Airflow se genera aleatoriamente y se guarda en SSM Parameter Store (`SecureString`); la de PostgreSQL se genera en la instancia; `terraform.tfvars.example` no tiene secretos |
| XML seguro | `defusedxml` bloquea entidades externas (XXE) y expansión de entidades |
| Integridad | SHA-256 de cada archivo Bronze, verificado antes de cada reprocesamiento |
| Protección de datos | `force_destroy = false` en el bucket: `terraform destroy` no borra datos sin vaciarlo antes |

**Nota sobre el estado de Terraform**: `terraform.tfstate` queda en su computador y contiene la
contraseña generada de Airflow. No lo suba al repositorio (está en `.gitignore`).
