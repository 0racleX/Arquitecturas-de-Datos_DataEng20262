resource "aws_iam_role" "airflow_ec2" {
  name = "${var.project}-airflow-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Session Manager: acceso a la instancia y port forwarding de la UI sin abrir puertos
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.airflow_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

locals {
  glue_arns = [
    "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
    "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
    "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*",
  ]
}

resource "aws_iam_role_policy" "pipeline" {
  name = "pipeline-access"
  role = aws_iam_role.airflow_ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "LakeBucketList"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketVersions"]
        Resource = aws_s3_bucket.lake.arn
      },
      {
        Sid    = "LakeBucketObjects"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion",
        "s3:AbortMultipartUpload"]
        Resource = "${aws_s3_bucket.lake.arn}/*"
      },
      {
        Sid    = "Comprehend" # fase de enriquecimiento (Silver)
        Effect = "Allow"
        Action = ["comprehend:DetectEntities", "comprehend:DetectKeyPhrases",
          "comprehend:BatchDetectEntities", "comprehend:BatchDetectKeyPhrases",
        "comprehend:DetectDominantLanguage", "comprehend:BatchDetectDominantLanguage"]
        Resource = "*" # las APIs síncronas de Comprehend no admiten restricción por recurso
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables",
          "glue:CreateTable", "glue:UpdateTable", "glue:GetPartition", "glue:GetPartitions",
          "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition",
          "glue:DeletePartition", "glue:BatchDeletePartition", "glue:BatchUpdatePartition",
        "glue:BatchGetPartition"]
        Resource = local.glue_arns
      },
      {
        Sid    = "Athena"
        Effect = "Allow"
        Action = ["athena:StartQueryExecution", "athena:GetQueryExecution",
          "athena:GetQueryResults", "athena:StopQueryExecution", "athena:GetWorkGroup",
        "athena:GetQueryRuntimeStatistics"]
        Resource = aws_athena_workgroup.lake.arn
      },
      {
        Sid      = "AirflowAdminPassword"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = aws_ssm_parameter.airflow_admin_password.arn
      }
    ]
  })
}

resource "aws_iam_instance_profile" "airflow_ec2" {
  name = "${var.project}-airflow-ec2"
  role = aws_iam_role.airflow_ec2.name
}
