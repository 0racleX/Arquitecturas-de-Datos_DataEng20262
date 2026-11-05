# ---------- Contraseñas generadas (nunca se escriben en el repositorio) ----------
# Sin caracteres especiales: RDS, DocumentDB y Redshift prohíben algunos (/ @ " ') y así
# la URI de DocumentDB no necesita escapes.

resource "random_password" "mysql" {
  length  = 24
  special = false
}

resource "random_password" "docdb" {
  length  = 24
  special = false
}

resource "random_password" "redshift" {
  length      = 24
  special     = false
  min_upper   = 2
  min_lower   = 2
  min_numeric = 2
}

# ---------- RDS for MySQL (fuente relacional) ----------

resource "aws_db_subnet_group" "mysql" {
  name       = "${var.project_name}-mysql"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_db_instance" "mysql" {
  identifier              = "${var.project_name}-mysql"
  engine                  = "mysql"
  engine_version          = "8.4"
  instance_class          = var.rds_instance_class
  allocated_storage       = 20
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "ecommerce"
  username                = "etl_admin"
  password                = random_password.mysql.result
  db_subnet_group_name    = aws_db_subnet_group.mysql.name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  publicly_accessible     = false
  multi_az                = false
  backup_retention_period = 1
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true
}

# ---------- Amazon DocumentDB (fuente documental) ----------

resource "aws_docdb_subnet_group" "main" {
  name       = "${var.project_name}-docdb"
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_docdb_cluster" "main" {
  cluster_identifier              = "${var.project_name}-docdb"
  engine                          = "docdb"
  engine_version                  = "5.0.0"
  master_username                 = "docdbadmin"
  master_password                 = random_password.docdb.result
  db_subnet_group_name            = aws_docdb_subnet_group.main.name
  vpc_security_group_ids          = [aws_security_group.docdb.id]
  storage_encrypted               = true
  backup_retention_period         = 1
  skip_final_snapshot             = true
  deletion_protection             = false
  apply_immediately               = true
  enabled_cloudwatch_logs_exports = []
}

resource "aws_docdb_cluster_instance" "main" {
  identifier         = "${var.project_name}-docdb-1"
  cluster_identifier = aws_docdb_cluster.main.id
  instance_class     = var.docdb_instance_class
  apply_immediately  = true
}

# ---------- Amazon Redshift Serverless (data warehouse) ----------

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name       = "${var.project_name}-ns"
  db_name              = "ecommerce_dw"
  admin_username       = local.redshift_admin_username
  admin_user_password  = random_password.redshift.result
  iam_roles            = [aws_iam_role.redshift.arn]
  default_iam_role_arn = aws_iam_role.redshift.arn
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.project_name}-wg"
  base_capacity  = var.redshift_base_capacity
  # Público para que QuickSight entre por su rango de IP; el security group limita el acceso.
  publicly_accessible = true
  subnet_ids          = aws_subnet.public[*].id
  security_group_ids  = [aws_security_group.redshift.id]
}

resource "aws_redshiftserverless_usage_limit" "compute" {
  resource_arn  = aws_redshiftserverless_workgroup.main.arn
  usage_type    = "serverless-compute"
  amount        = var.redshift_daily_rpu_hours_limit
  period        = "daily"
  breach_action = var.redshift_usage_breach_action
}

locals {
  redshift_admin_username = "dwadmin"
  redshift_host           =aws_redshiftserverless_workgroup.main.endpoint[0].address
}
