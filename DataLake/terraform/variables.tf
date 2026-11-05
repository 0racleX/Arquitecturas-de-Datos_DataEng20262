variable "aws_region" {
  description = "Región AWS (Comprehend soporta español en us-east-1)"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Prefijo de nombres"
  type        = string
  default     = "news-datalake"
}

variable "instance_type" {
  description = "Airflow + Postgres en la misma máquina: t3.medium es el mínimo cómodo"
  type        = string
  default     = "t3.medium"
}

variable "allowed_cidrs" {
  description = <<-EOT
    CIDRs con acceso a la UI de Airflow (8080) y SSH (22). Vacío = sin puertos abiertos;
    se accede por SSM Session Manager con port forwarding (recomendado).
  EOT
  type        = list(string)
  default     = []
}

variable "key_name" {
  description = "Key pair EC2 opcional para SSH"
  type        = string
  default     = null
}

variable "airflow_version" {
  type    = string
  default = "2.10.5"
}

variable "glue_database_name" {
  type    = string
  default = "news_datalake"
}

variable "athena_results_retention_days" {
  type    = number
  default = 7
}

variable "schedule" {
  description = "Cron del DAG durante el laboratorio (UTC). Por defecto cada hora."
  type        = string
  default     = "0 * * * *"
}
