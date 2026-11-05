variable "aws_region" {
  description = "Región de AWS donde se despliega todo."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefijo de nombres de los recursos (minúsculas, números y guiones)."
  type        = string
  default     = "de-dw"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project_name))
    error_message = "project_name debe tener entre 3 y 21 caracteres: minúsculas, números y guiones."
  }
}

variable "allowed_cidr" {
  description = "CIDR que puede entrar por SSH y a la UI de Airflow. Vacío = tu IP pública actual (/32)."
  type        = string
  default     = ""
}

variable "ec2_instance_type" {
  description = "Instancia que ejecuta Airflow y dbt (Docker). t3.large = 2 vCPU / 8 GiB."
  type        = string
  default     = "t3.large"
}

variable "rds_instance_class" {
  description = "Clase de la instancia RDS for MySQL."
  type        = string
  default     = "db.t4g.micro"
}

variable "docdb_instance_class" {
  description = "Clase de la instancia de DocumentDB (db.t3.medium es la más barata)."
  type        = string
  default     = "db.t3.medium"
}

variable "redshift_base_capacity" {
  description = "RPU base de Redshift Serverless (mínimo 8)."
  type        = number
  default     = 8
}

variable "redshift_daily_rpu_hours_limit" {
  description = "Tope diario de RPU-hora de Redshift Serverless; al superarlo el workgroup se desactiva."
  type        = number
  default     = 24
}

variable "redshift_usage_breach_action" {
  description = "Acción al superar el tope: log, emit-metric o deactivate."
  type        = string
  default     = "deactivate"
}

variable "quicksight_ip_cidrs" {
  description = "Rango de IP de QuickSight por región (documentación de AWS: 'AWS Regions, websites, IP address ranges, and endpoints')."
  type        = map(string)
  default = {
    "us-east-1" = "52.23.63.224/27"
    "us-east-2" = "52.15.247.160/27"
    "us-west-2" = "54.70.204.128/27"
    "eu-west-1" = "52.210.255.224/27"
  }
}

variable "enable_quicksight" {
  description = "Crea el data source y el dataset de QuickSight. Requiere la suscripción de QuickSight activa y que el pipeline ya haya creado marts.rpt_sales_dashboard."
  type        = bool
  default     = false
}

variable "quicksight_user_arn" {
  description = "ARN del usuario de QuickSight dueño del data source/dataset, p. ej. arn:aws:quicksight:us-east-1:123456789012:user/default/mi-usuario."
  type        = string
  default     = ""
}
