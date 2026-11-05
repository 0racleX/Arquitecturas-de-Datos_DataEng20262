select
    order_id,
    customer_id,
    order_date,
    status,
    sales_channel,
    payment_method,
    total,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from {{ ref('int_mysql__orders__qa') }}
where rejection_reason is null
