# Opcional (enable_quicksight = true). Requisitos:
#   1. Suscripción de QuickSight activa en la cuenta (se hace una vez desde la consola).
#   2. El pipeline ya ejecutado, para que exista la vista marts.rpt_sales_dashboard.
# El dashboard (gráficas, KPI y filtro) se arma en la consola sobre este dataset.

locals {
  quicksight_data_source_actions = [
    "quicksight:DescribeDataSource",
    "quicksight:DescribeDataSourcePermissions",
    "quicksight:PassDataSource",
    "quicksight:UpdateDataSource",
    "quicksight:DeleteDataSource",
    "quicksight:UpdateDataSourcePermissions",
  ]
  quicksight_data_set_actions = [
    "quicksight:DescribeDataSet",
    "quicksight:DescribeDataSetPermissions",
    "quicksight:PassDataSet",
    "quicksight:DescribeIngestion",
    "quicksight:ListIngestions",
    "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSet",
    "quicksight:CreateIngestion",
    "quicksight:CancelIngestion",
    "quicksight:UpdateDataSetPermissions",
  ]
}

resource "aws_quicksight_data_source" "redshift" {
  count          = var.enable_quicksight ? 1 : 0
  data_source_id = "${var.project_name}-redshift"
  name           = "DE-DW Redshift"
  type           = "REDSHIFT"

  parameters {
    redshift {
      host     = local.redshift_host
      port     = 5439
      database = aws_redshiftserverless_namespace.main.db_name
    }
  }

  credentials {
    credential_pair {
      username = local.redshift_admin_username
      password = random_password.redshift.result
    }
  }

  ssl_properties {
    disable_ssl = false
  }

  permission {
    principal = var.quicksight_user_arn
    actions   = local.quicksight_data_source_actions
  }
}

resource "aws_quicksight_data_set" "sales" {
  count       = var.enable_quicksight ? 1 : 0
  data_set_id = "${var.project_name}-sales"
  name        = "DE-DW ventas (marts.rpt_sales_dashboard)"
  import_mode = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "sales"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.redshift[0].arn
      name            = "rpt_sales_dashboard"
      sql_query       = "select * from marts.rpt_sales_dashboard"

      columns {
        name = "sale_sk"
        type = "STRING"
      }
      columns {
        name = "sale_date"
        type = "DATETIME"
      }
      columns {
        name = "year"
        type = "INTEGER"
      }
      columns {
        name = "month"
        type = "INTEGER"
      }
      columns {
        name = "category"
        type = "STRING"
      }
      columns {
        name = "product_name"
        type = "STRING"
      }
      columns {
        name = "department"
        type = "STRING"
      }
      columns {
        name = "city"
        type = "STRING"
      }
      columns {
        name = "sales_channel"
        type = "STRING"
      }
      columns {
        name = "quantity"
        type = "INTEGER"
      }
      columns {
        name = "subtotal"
        type = "DECIMAL"
      }
      columns {
        name = "model_refreshed_at"
        type = "DATETIME"
      }
    }
  }

  permissions {
    principal = var.quicksight_user_arn
    actions   = local.quicksight_data_set_actions
  }
}
