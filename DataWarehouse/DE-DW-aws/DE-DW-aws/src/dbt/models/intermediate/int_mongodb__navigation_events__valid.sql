select
    event_id,
    event_type,
    occurred_at,
    customer_id,
    product_id,
    sales_channel,
    document_json,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from {{ ref('int_mongodb__navigation_events__qa') }}
where rejection_reason is null
