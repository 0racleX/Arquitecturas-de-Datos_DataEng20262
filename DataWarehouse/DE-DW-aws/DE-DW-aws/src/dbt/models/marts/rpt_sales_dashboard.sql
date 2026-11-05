{{ config(materialized='view', bind=false) }}
-- Vista que consume QuickSight: una fila por línea de venta con sus atributos de análisis.
-- model_refreshed_at es el momento en que dbt reconstruyó el modelo (fecha de actualización del tablero).

select
    sales.sale_sk,
    dates.date as sale_date,
    dates.year,
    dates.month,
    products.category,
    products.name as product_name,
    customers.department,
    customers.city,
    channels.sales_channel,
    sales.quantity,
    sales.subtotal,
    cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as model_refreshed_at
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as dates on dates.date_sk = sales.date_sk
inner join {{ ref('dim_product') }} as products on products.product_sk = sales.product_sk
inner join {{ ref('dim_customer') }} as customers on customers.customer_sk = sales.customer_sk
inner join {{ ref('dim_sales_channel') }} as channels on channels.sales_channel_sk = sales.sales_channel_sk
