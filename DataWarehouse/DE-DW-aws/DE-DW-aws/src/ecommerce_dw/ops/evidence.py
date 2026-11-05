#!/usr/bin/env python3
"""Consultas de evidencia sobre Redshift para las capturas y la verificación de escenarios.

    python -m ecommerce_dw.ops.evidence all
    python -m ecommerce_dw.ops.evidence sql "select ..."
"""

import hashlib
import sys

from ecommerce_dw.ingestion import redshift

SECTIONS = {
    "batches": [
        ("Lotes registrados (ops.batch_control)",
         "select batch_id, mode, status, started_at, finished_at, failed_task, left(message, 120) as message "
         "from ops.batch_control order by started_at"),
        ("Extracciones por lote y entidad (ops.extraction_log)",
         "select batch_id, source, entity, rows_extracted, rows_loaded, watermark_column, watermark_from, "
         "watermark_to from ops.extraction_log order by extracted_at, source, entity"),
    ],
    "raw": [
        ("Filas en raw_data por lote",
         "select 'mysql_customers' as tabla, _batch_id, count(*) as filas from raw_data.mysql_customers group by 2 "
         "union all select 'mysql_products', _batch_id, count(*) from raw_data.mysql_products group by 2 "
         "union all select 'mysql_orders', _batch_id, count(*) from raw_data.mysql_orders group by 2 "
         "union all select 'mysql_order_items', _batch_id, count(*) from raw_data.mysql_order_items group by 2 "
         "union all select 'mongo_reviews', _batch_id, count(*) from raw_data.mongo_reviews group by 2 "
         "union all select 'mongo_navigation_events', _batch_id, count(*) from raw_data.mongo_navigation_events group by 2 "
         "order by 2, 1"),
    ],
    "quarantine": [
        ("Rechazos por tabla, motivo y lote de origen",
         "select 'mysql_customers' as tabla, rejection_reason, _first_batch_id as lote_origen, count(*) as filas from quarantine.quarantine_mysql__customers group by 2, 3 "
         "union all select 'mysql_products', rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mysql__products group by 2, 3 "
         "union all select 'mysql_orders', rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mysql__orders group by 2, 3 "
         "union all select 'mysql_order_items', rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mysql__order_items group by 2, 3 "
         "union all select 'mongo_reviews', rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mongodb__reviews group by 2, 3 "
         "union all select 'mongo_navigation_events', rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mongodb__navigation_events group by 2, 3 "
         "order by 1, 2"),
    ],
    "facts": [
        ("Hechos: filas y llaves distintas",
         "select 'fct_sales' as hecho, count(*) as filas, count(distinct sale_sk) as llaves, sum(subtotal) as total from marts.fct_sales "
         "union all select 'fct_reviews', count(*), count(distinct review_sk), null from marts.fct_reviews "
         "union all select 'fct_navigation_events', count(*), count(distinct navigation_event_sk), null from marts.fct_navigation_events"),
    ],
    "scd2": [
        ("SCD2 cliente 1 (dim_customer)",
         "select customer_sk, customer_id, city, department, valid_from, valid_to, is_current "
         "from marts.dim_customer where customer_id = 1 order by valid_from"),
        ("SCD2 producto 1 (dim_product)",
         "select product_sk, product_id, category, valid_from, valid_to, is_current "
         "from marts.dim_product where product_id = 1 order by valid_from"),
    ],
    "reconcile": [
        ("Conciliación: ventas válidas de origen vs. fct_sales",
         "with origen as (select count(*) as lineas, sum(items.subtotal) as total "
         "from intermediate.int_mysql__order_items__valid as items "
         "inner join intermediate.int_mysql__orders__valid as orders on orders.order_id = items.order_id), "
         "modelo as (select count(*) as lineas, sum(subtotal) as total from marts.fct_sales) "
         "select origen.lineas as lineas_origen, modelo.lineas as lineas_modelo, origen.total as total_origen, "
         "modelo.total as total_modelo, origen.total = modelo.total as coincide from origen cross join modelo"),
    ],
    "dashboard": [
        ("Indicador del dashboard: ventas totales (marts.rpt_sales_dashboard)",
         "select sum(subtotal) as ventas_totales, count(*) as lineas, max(model_refreshed_at) as actualizado "
         "from marts.rpt_sales_dashboard"),
        ("Ventas por categoría",
         "select category, sum(subtotal) as ventas from marts.rpt_sales_dashboard group by 1 order by 2 desc"),
    ],
}
FACTS = [("fct_sales", "sale_sk", "subtotal"), ("fct_reviews", "review_sk", None),
         ("fct_navigation_events", "navigation_event_sk", None)]
VALID_SOURCES = [
    ("valid_order_items", "select count(*), coalesce(sum(subtotal), 0) from intermediate.int_mysql__order_items__valid"),
    ("valid_reviews", "select count(*), 0 from intermediate.int_mongodb__reviews__valid"),
    ("valid_navigation_events", "select count(*), 0 from intermediate.int_mongodb__navigation_events__valid"),
]
SNAPSHOTS = [("snap_dim_customer", "customer_id"), ("snap_dim_product", "product_id")]


def print_table(title, columns, rows):
    print(f"\n=== {title} ===")
    if not columns:
        print("(sin resultado)")
        return
    text_rows = [["" if value is None else str(value) for value in row] for row in rows]
    widths = [max([len(column)] + [len(row[index]) for row in text_rows]) for index, column in enumerate(columns)]
    print(" | ".join(column.ljust(width) for column, width in zip(columns, widths)))
    print("-+-".join("-" * width for width in widths))
    for row in text_rows:
        print(" | ".join(value.ljust(width) for value, width in zip(row, widths)))
    print(f"({len(rows)} filas)")


def run_section(name):
    for title, sql in SECTIONS[name]:
        try:
            columns, rows = redshift.query(sql)
            print_table(title, columns, rows)
        except Exception as error:  # noqa: BLE001 - un esquema aún no creado no debe ocultar el resto
            print(f"\n=== {title} ===\nNo disponible: {error}")


def state():
    """Una línea por hecho (filas, llaves, total, huella del contenido), fuente validada y snapshot."""
    for table, key, amount in FACTS:
        amount_sql = f"coalesce(sum({amount}), 0)" if amount else "0"
        _, rows = redshift.query(f"select count(*), count(distinct {key}), {amount_sql} from marts.{table}")
        count, unique_keys, total = rows[0]
        _, content = redshift.query(f"select * from marts.{table} order by {key}")
        digest = hashlib.sha256(repr([tuple(row) for row in content]).encode()).hexdigest()[:16]
        print(table, count, unique_keys, total, digest)
    for name, sql in VALID_SOURCES:
        _, rows = redshift.query(sql)
        count, total = rows[0]
        print(name, count, count, total, "-")
    for table, key in SNAPSHOTS:
        _, rows = redshift.query(f"select count(*), count(distinct {key}) from snapshots.{table}")
        print(table, rows[0][0], rows[0][1], 0, "-")


def reset_warehouse():
    redshift.execute([(f"drop schema if exists {schema} cascade", None)
                      for schema in ("marts", "snapshots", "quarantine", "intermediate", "raw_data", "ops")])
    print("Redshift schemas dropped.")


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    if command == "sql":
        columns, rows = redshift.query(sys.argv[2])
        print_table("Consulta", columns, rows)
    elif command == "state":
        state()
    elif command == "reset-warehouse":
        reset_warehouse()
    elif command == "all":
        for name in SECTIONS:
            run_section(name)
    elif command in SECTIONS:
        run_section(command)
    else:
        raise SystemExit(f"Unknown command {command!r}. Use: all, sql, state, reset-warehouse, {', '.join(SECTIONS)}")


if __name__ == "__main__":
    main()
