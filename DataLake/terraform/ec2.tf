data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

data "aws_vpc" "default" { default = true }

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "random_password" "airflow_admin" {
  length  = 24
  special = false
}

resource "aws_ssm_parameter" "airflow_admin_password" {
  name  = "/${var.project}/airflow/admin_password"
  type  = "SecureString"
  value = random_password.airflow_admin.result
}

resource "aws_security_group" "airflow" {
  name        = "${var.project}-airflow"
  description = "Airflow en EC2"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = length(var.allowed_cidrs) > 0 ? [8080, 22] : []
    content {
      description = "Airflow UI / SSH"
      from_port   = ingress.value
      to_port     = ingress.value
      protocol    = "tcp"
      cidr_blocks = var.allowed_cidrs
    }
  }

  egress {
    description = "Salida a feeds RSS, PyPI y APIs AWS"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ssm_parameter.al2023.value
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.airflow.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow_ec2.name
  key_name               = var.key_name

  metadata_options {
    http_tokens = "required" # IMDSv2
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    bucket          = aws_s3_bucket.lake.bucket
    region          = var.aws_region
    airflow_version = var.airflow_version
    password_param  = aws_ssm_parameter.airflow_admin_password.name
    glue_database   = var.glue_database_name
    athena_wg       = aws_athena_workgroup.lake.name
    schedule        = var.schedule
  })
  user_data_replace_on_change = true

  # El código debe estar en S3 antes de que la instancia arranque
  depends_on = [aws_s3_object.code, aws_iam_role_policy.pipeline]

  tags = { Name = "${var.project}-airflow" }
}
