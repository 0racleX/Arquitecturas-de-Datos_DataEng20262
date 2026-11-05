-- Validación del indicador "Ventas totales" del dashboard de QuickSight.
-- Ejecutar en Redshift Query editor v2 (workgroup de-dw-wg, base ecommerce_dw) con los MISMOS filtros
-- aplicados en el dashboard. model_refreshed_at debe coincidir con el valor mostrado en el tablero.

-- 1. Sin filtros: debe ser igual al KPI del dashboard sin filtros.
select
    sum(subtotal)           as ventas_totales,
    count(*)                as lineas_de_venta,
    max(model_refreshed_at) as fecha_actualizacion
from marts.rpt_sales_dashboard;

-- 2. Con el filtro del dashboard (cambia el valor por el seleccionado en el control).
select
    sum(subtotal)           as ventas_totales,
    count(*)                as lineas_de_venta,
    max(model_refreshed_at) as fecha_actualizacion
from marts.rpt_sales_dashboard
where category = 'Electrónica';

-- 3. Serie de la gráfica (ventas por mes y categoría).
select
    date_trunc('month', sale_date) as mes,
    category,
    sum(subtotal)                  as ventas
from marts.rpt_sales_dashboard
group by 1, 2
order by 1, 2;

-- 4. Conciliación del indicador contra la fuente validada (no depende de QuickSight).
with origen as (
    select sum(items.subtotal) as total
    from intermediate.int_mysql__order_items__valid as items
    inner join intermediate.int_mysql__orders__valid as orders on orders.order_id = items.order_id
),
modelo as (
    select sum(subtotal) as total from marts.rpt_sales_dashboard
)
select origen.total as total_origen, modelo.total as total_modelo, origen.total = modelo.total as coincide
from origen cross join modelo;
