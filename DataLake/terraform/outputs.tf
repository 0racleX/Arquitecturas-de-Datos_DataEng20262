output "bucket_name" {
  value = aws_s3_bucket.lake.bucket
}

output "instance_id" {
  value = aws_instance.airflow.id
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}

output "airflow_admin_password_cmd" {
  description = "Comando para leer la contraseña del usuario admin de Airflow"
  value       = "aws ssm get-parameter --name ${aws_ssm_parameter.airflow_admin_password.name} --with-decryption --query Parameter.Value --output text"
}

output "airflow_ui_tunnel_cmd" {
  description = "Abre la UI de Airflow en http://localhost:8080 vía Session Manager"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id} --document-name AWS-StartPortForwardingSession --parameters portNumber=8080,localPortNumber=8080"
}

output "ec2_shell_cmd" {
  description = "Abre una consola en la EC2 (luego: sudo -iu airflow)"
  value       = "aws ssm start-session --target ${aws_instance.airflow.id}"
}
