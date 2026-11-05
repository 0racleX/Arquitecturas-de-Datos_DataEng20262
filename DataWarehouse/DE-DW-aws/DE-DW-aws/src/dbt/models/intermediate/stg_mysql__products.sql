-- Versión más reciente de cada llave entre todos los lotes cargados en raw_data.
with latest as (
    {{ latest_per_key(source('mysql_raw', 'mysql_products'), 'product_id') }}
)

select
    cast(product_id as bigint) as product_id,
    nullif(trim(name), '') as name,
    nullif(trim(category), '') as category,
    cast(price as decimal(12, 2)) as price,
    nullif(trim(description), '') as description,
    cast(is_active as boolean) as is_active,
    cast(updated_at as timestamp) as updated_at,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from latest
