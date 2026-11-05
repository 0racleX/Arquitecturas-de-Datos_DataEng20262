-- Versión más reciente de cada llave entre todos los lotes cargados en raw_data.
with latest as (
    {{ latest_per_key(source('mongodb_raw', 'mongo_reviews'), 'review_id') }}
)

select
    cast(review_id as bigint) as review_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(rating as integer) as rating,
    -- reviewed_at llega en UTC desde DocumentDB.
    cast(reviewed_at as timestamp) as reviewed_at,
    lower(json_extract_path_text(document_json, 'verification', 'purchase_confirmed', true)) = 'true' as is_verified_purchase,
    document_json,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from latest
