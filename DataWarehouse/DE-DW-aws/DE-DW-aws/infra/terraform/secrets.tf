# Todas las credenciales viven en un único secreto de Secrets Manager. La EC2 lo lee con su
# rol IAM al arrancar y genera /opt/de-dw/.env (chmod 600); el repositorio no guarda secretos.

resource "random_password" "airflow_admin" {
  length  = 20
  special = false
}

resource "random_password" "airflow_secret_key" {
  length  = 40
  special = false
}

resource "random_password" "postgres" {
  length  = 24
  special = false
}

resource "random_bytes" "fernet" {
  length = 32
}

locals {
  docdb_uri = format(
    "mongodb://%s:%s@%s:27017/?tls=true&tlsCAFile=/opt/airflow/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false",
    aws_docdb_cluster.main.master_username,
    random_password.docdb.result,
    aws_docdb_cluster.main.endpoint,
  )

  connection_env = {
    AIRFLOW_UID            = "1000"
    AIRFLOW_UI_PORT        = "8080"
    AIRFLOW_FERNET_KEY     = replace(replace(random_bytes.fernet.base64, "+", "-"), "/", "_")
    AIRFLOW_SECRET_KEY     = random_password.airflow_secret_key.result
    AIRFLOW_ADMIN_USERNAME = "airflow"
    AIRFLOW_ADMIN_PASSWORD = random_password.airflow_admin.result
    POSTGRES_USER          = "airflow"
    POSTGRES_PASSWORD      = random_password.postgres.result

    MYSQL_HOST     = aws_db_instance.mysql.address
    MYSQL_PORT     = tostring(aws_db_instance.mysql.port)
    MYSQL_DATABASE = aws_db_instance.mysql.db_name
    MYSQL_USER     = aws_db_instance.mysql.username
    MYSQL_PASSWORD = random_password.mysql.result

    MONGODB_URI    = local.docdb_uri
    MONGO_DATABASE = "ecommerce"

    REDSHIFT_HOST         = local.redshift_host
    REDSHIFT_PORT         = "5439"
    REDSHIFT_DATABASE     = aws_redshiftserverless_namespace.main.db_name
    REDSHIFT_USER         = local.redshift_admin_username
    REDSHIFT_PASSWORD     = random_password.redshift.result
    REDSHIFT_IAM_ROLE_ARN = aws_iam_role.redshift.arn

    DE_DW_BUCKET       = aws_s3_bucket.lake.id
    AWS_REGION         = var.aws_region
    AWS_DEFAULT_REGION = var.aws_region
  }
}

resource "aws_secretsmanager_secret" "connections" {
  name                    = "${var.project_name}/connections"
  description             = "Credenciales y endpoints del pipeline DE-DW en AWS"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "connections" {
  secret_id     = aws_secretsmanager_secret.connections.id
  secret_string = jsonencode(local.connection_env)
}
