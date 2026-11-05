output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}

output "airflow_url" {
  value = "http://${aws_instance.airflow.public_ip}:8080"
}

output "airflow_admin_username" {
  value = "airflow"
}

output "airflow_admin_password" {
  value     = random_password.airflow_admin.result
  sensitive = true
}

output "ssh_command" {
  value = "ssh -i ${local_sensitive_file.ssh_private_key.filename} ubuntu@${aws_instance.airflow.public_ip}"
}

output "ssh_private_key_path" {
  value = local_sensitive_file.ssh_private_key.filename
}

output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "connections_secret_id" {
  value = aws_secretsmanager_secret.connections.id
}

output "rds_endpoint" {
  value = aws_db_instance.mysql.address
}

output "docdb_endpoint" {
  value = aws_docdb_cluster.main.endpoint
}

output "redshift_endpoint" {
  value = local.redshift_host
}

output "redshift_database" {
  value = aws_redshiftserverless_namespace.main.db_name
}

output "redshift_admin_username" {
  value = local.redshift_admin_username
}

output "redshift_admin_password" {
  value     = random_password.redshift.result
  sensitive = true
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift.arn
}

output "allowed_cidr" {
  value = local.allowed_cidr
}
