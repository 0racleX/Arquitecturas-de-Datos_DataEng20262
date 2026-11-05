-- Versión más reciente de cada llave entre todos los lotes cargados en raw_data.
with latest as (
    {{ latest_per_key(source('mongodb_raw', 'mongo_navigation_events'), 'event_id') }}
)

select
    cast(event_id as bigint) as event_id,
    nullif(trim(event_type), '') as event_type,
    -- occurred_at llega en UTC desde DocumentDB.
    cast(occurred_at as timestamp) as occurred_at,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    nullif(trim(sales_channel), '') as sales_channel,
    document_json,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from latest
