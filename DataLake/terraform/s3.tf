# Bucket ÚNICO del data lake: todas las zonas son prefijos de este bucket.
#   bronze/  silver/  gold/          -> arquitectura medallón
#   quarantine/                      -> respuestas HTTP inválidas (bronze/) y registros rechazados (silver/)
#   control/                         -> manifiestos, punteros de commit, logs de lotes, estado y calidad
#   staging/ work/                   -> intermedios (no consultables); work/ expira a los 7 días
#   athena-results/                  -> resultados de consultas Athena
#   artifacts/                       -> código (DAGs + paquete Python) que la EC2 sincroniza

locals {
  bucket_name = "${var.project}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  zones       = ["bronze/", "silver/", "gold/", "staging/", "quarantine/", "control/", "athena-results/"]
  code_root   = "${path.module}/.."
  code_files = setunion(
    fileset(local.code_root, "dags/**/*.py"),
    fileset(local.code_root, "src/**/*.py"),
    fileset(local.code_root, "config/*.yaml"),
  )
}

resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = false # protege los datos ante un `terraform destroy` accidental
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versionado: recuperación ante sobrescrituras o borrados accidentales en Bronze
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

# Solo HTTPS
resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_public_access_block.lake]
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  rule {
    id     = "athena-results-expire"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = var.athena_results_retention_days }
  }

  rule {
    id     = "bronze-quarantine-expire" # respuestas HTTP inválidas; la cuarentena de Silver se conserva
    status = "Enabled"
    filter { prefix = "quarantine/bronze/" }
    expiration { days = 30 }
  }

  rule {
    id     = "work-expire"
    status = "Enabled"
    filter { prefix = "work/" }
    expiration { days = 7 }
  }

  rule {
    id     = "noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration { noncurrent_days = 30 }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

# Marcadores de carpeta para que las zonas sean visibles en la consola
resource "aws_s3_object" "zones" {
  for_each = toset(local.zones)
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""

  # Evita el 404 de S3 recién creado: sube objetos solo cuando el bucket está configurado
  depends_on = [aws_s3_bucket_versioning.lake, aws_s3_bucket_server_side_encryption_configuration.lake,
  aws_s3_bucket_policy.lake, aws_s3_bucket_lifecycle_configuration.lake]
}

# Código del pipeline -> artifacts/ (la EC2 lo sincroniza cada 2 min)
resource "aws_s3_object" "code" {
  for_each = local.code_files
  bucket   = aws_s3_bucket.lake.id
  key      = "artifacts/news-datalake/${each.value}"
  source   = "${local.code_root}/${each.value}"
  etag     = filemd5("${local.code_root}/${each.value}")

  # Evita el 404 de S3 recién creado: sube objetos solo cuando el bucket está configurado
  depends_on = [aws_s3_bucket_versioning.lake, aws_s3_bucket_server_side_encryption_configuration.lake,
  aws_s3_bucket_policy.lake, aws_s3_bucket_lifecycle_configuration.lake]
}
