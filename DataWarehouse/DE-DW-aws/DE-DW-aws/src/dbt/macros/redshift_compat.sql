{# Redshift no soporta IS DISTINCT FROM: comparación equivalente que trata dos nulos como iguales. #}
{% macro is_distinct_from(left, right) -%}
(({{ left }} <> {{ right }}) or ({{ left }} is null and {{ right }} is not null) or ({{ left }} is not null and {{ right }} is null))
{%- endmacro %}

{# Hora local de negocio a partir de un TIMESTAMP en UTC (sustituye timezone() de DuckDB). #}
{% macro to_business_time(utc_timestamp) -%}
convert_timezone('UTC', '{{ var("business_timezone") }}', {{ utc_timestamp }})
{%- endmacro %}

{# Raw guarda todas las versiones extraídas (una por lote). Staging toma la más reciente por llave:
   _batch_id es el último lote que la trajo y _first_batch_id el primero. #}
{% macro latest_per_key(relation, key_column) -%}
select *
from (
    select
        *,
        row_number() over (
            partition by {{ key_column }}
            order by _extracted_at desc, _batch_id desc
        ) as _version_rank,
        -- Lote en el que la llave apareció por primera vez (lote de origen del registro).
        first_value(_batch_id) over (
            partition by {{ key_column }}
            order by _extracted_at, _batch_id
            rows between unbounded preceding and unbounded following
        ) as _first_batch_id
    from {{ relation }}
) as versions
where _version_rank = 1
{%- endmacro %}
