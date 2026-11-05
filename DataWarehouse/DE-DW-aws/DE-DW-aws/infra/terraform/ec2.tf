data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "tls_private_key" "ssh" {
  algorithm = "ED25519"
}

resource "aws_key_pair" "ssh" {
  key_name   = "${var.project_name}-key"
  public_key = tls_private_key.ssh.public_key_openssh
}

resource "local_sensitive_file" "ssh_private_key" {
  content         = tls_private_key.ssh.private_key_openssh
  filename        = abspath("${path.module}/${var.project_name}-key.pem")
  file_permission = "0600"
}

resource "aws_instance" "airflow" {
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.ec2_instance_type
  subnet_id                   = aws_subnet.public[0].id
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  key_name                    = aws_key_pair.ssh.key_name
  associate_public_ip_address = true

  # IMDSv2 obligatorio; hop limit 2 para que los contenedores obtengan las credenciales del rol.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    aws_region = var.aws_region
    bucket     = aws_s3_bucket.lake.id
    code_key   = aws_s3_object.code.key
    secret_id  = aws_secretsmanager_secret.connections.id
  })
  user_data_replace_on_change = true

  # Las fuentes y el warehouse deben existir antes del arranque: el bootstrap genera los datos.
  depends_on = [
    aws_db_instance.mysql,
    aws_docdb_cluster_instance.main,
    aws_redshiftserverless_workgroup.main,
    aws_secretsmanager_secret_version.connections,
    aws_s3_object.code,
    aws_iam_role_policy.ec2_pipeline,
  ]

  tags = { Name = "${var.project_name}-airflow" }
}
