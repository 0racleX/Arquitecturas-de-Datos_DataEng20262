data "aws_caller_identity" "current" {}

# Bucket privado del data lake raw. force_destroy permite que `terraform destroy` lo vacíe.
resource "aws_s3_bucket" "lake" {
  bucket        = "${var.project_name}-lake-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# El versionado conserva cualquier objeto reemplazado por error.
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_policy" "lake_tls_only" {
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

# Código del repositorio empaquetado: la EC2 lo descarga al arrancar.
data "archive_file" "code" {
  type        = "zip"
  source_dir  = "${path.module}/../.."
  output_path = "${path.module}/.build/de-dw.zip"
  excludes = [
    ".git/**",
    ".env",
    "infra/terraform/.terraform/**",
    "infra/terraform/.build/**",
    "infra/terraform/*.tfstate*",
    "infra/terraform/terraform.tfvars",
    "infra/terraform/*.pem",
    "**/__pycache__/**",
    "src/dbt/target/**",
    "src/dbt/dbt_packages/**",
    "src/dbt/logs/**",
    "evidencias/**",
  ]
}

resource "aws_s3_object" "code" {
  bucket = aws_s3_bucket.lake.id
  key    = "artifacts/de-dw.zip"
  source = data.archive_file.code.output_path
  etag   = data.archive_file.code.output_md5
}
