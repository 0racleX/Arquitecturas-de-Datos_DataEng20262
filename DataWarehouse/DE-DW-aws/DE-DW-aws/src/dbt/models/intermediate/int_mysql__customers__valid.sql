select
    customer_id,
    name,
    email,
    phone,
    city,
    department,
    registration_date,
    updated_at,
    _batch_id,
    _first_batch_id,
    _extracted_at,
    _loaded_at,
    _source
from {{ ref('int_mysql__customers__qa') }}
where rejection_reason is null
