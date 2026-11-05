{# Filtro de cargas incrementales para la fila candidata (alias candidate). Carga las llaves nuevas, las filas cuya fecha de negocio cae en la ventana de reproceso (desde incremental_lookback_days antes de la fecha máxima ya cargada) y las filas cuyas llaves de dimensión resueltas cambiaron (tracked_columns), por ejemplo cuando una versión SCD2 nueva reemplaza a la anterior. #}
{% macro incremental_candidate_filter(key_column, business_date_column, tracked_columns=[]) %}
{# Redshift no admite el exists correlacionado con varios or: la comparación con la fila ya cargada se hace con un left join. #}
{% if is_incremental() %}
left join {{ this }} as existing
    on existing.{{ key_column }} = candidate.{{ key_column }}
where existing.{{ key_column }} is null
or candidate.{{ business_date_column }} >= (
    select dateadd(day, -{{ var('incremental_lookback_days') }}, max(loaded_dates.date))
    from {{ this }} as loaded
    inner join {{ ref('dim_date') }} as loaded_dates on loaded_dates.date_sk = loaded.date_sk
)
{% for column in tracked_columns %}
or {{ is_distinct_from('existing.' ~ column, 'candidate.' ~ column) }}
{% endfor %}
{% endif %}
{% endmacro %}

{# Elimina de la tabla de hechos las filas cuya llave de origen ya no existe en la capa validada. #}
{% macro delete_orphaned_facts(sk_column, source_relation, source_key) %}
delete from {{ this }}
where {{ sk_column }} not in (
    select {{ dbt_utils.generate_surrogate_key(['cast(' ~ source_key ~ ' as varchar)']) }}
    from {{ source_relation }}
)
{% endmacro %}
