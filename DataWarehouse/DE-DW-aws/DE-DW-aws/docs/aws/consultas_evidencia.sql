-- Consultas de evidencia para Redshift Query editor v2 (equivalen a scripts/warehouse_report.sh).

-- Estado de los lotes y marca de avance
select batch_id, mode, status, started_at, finished_at, failed_task, message
from ops.batch_control order by started_at;

select batch_id, source, entity, rows_extracted, rows_loaded, watermark_column, watermark_from, watermark_to, s3_uri
from ops.extraction_log order by extracted_at, source, entity;

-- Raw: filas por lote (raw conserva cada lote)
select _batch_id, count(*) from raw_data.mysql_orders group by 1 order by 1;

-- Rechazos con motivo y lote de origen
select rejection_reason, _first_batch_id as lote_origen, _batch_id as ultimo_lote, count(*)
from quarantine.quarantine_mysql__order_items group by 1, 2, 3 order by 1;
select rejection_reason, _first_batch_id, count(*) from quarantine.quarantine_mongodb__reviews group by 1, 2;

-- Unicidad de hechos (deben coincidir filas y llaves)
select count(*) as filas, count(distinct sale_sk) as llaves from marts.fct_sales;

-- SCD tipo 2
select customer_id, city, department, valid_from, valid_to, is_current
from marts.dim_customer where customer_id = 1 order by valid_from;
select product_id, category, valid_from, valid_to, is_current
from marts.dim_product where product_id = 1 order by valid_from;

-- Ventas por fecha de negocio: fuente validada vs. modelo
with origen as (
    select cast(orders.order_date as date) as fecha, sum(items.subtotal) as total
    from intermediate.int_mysql__order_items__valid as items
    inner join intermediate.int_mysql__orders__valid as orders on orders.order_id = items.order_id
    group by 1
),
modelo as (
    select dates.date as fecha, sum(sales.subtotal) as total
    from marts.fct_sales as sales
    inner join marts.dim_date as dates on dates.date_sk = sales.date_sk
    group by 1
)
select count(*) as dias, sum(case when origen.total = modelo.total then 1 else 0 end) as dias_que_coinciden
from origen full outer join modelo on modelo.fecha = origen.fecha;
