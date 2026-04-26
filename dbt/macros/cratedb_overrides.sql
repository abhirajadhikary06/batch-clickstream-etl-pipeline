{% macro cratedb__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from information_schema.columns
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro cratedb__drop_relation(relation) -%}
  {% call statement('lookup_relation_kind', fetch_result=True, auto_begin=False) %}
    select 'view' as kind
    from information_schema.views
    where table_schema = '{{ relation.schema }}'
      and table_name = '{{ relation.identifier }}'
    union all
    select 'table' as kind
    from information_schema.tables
    where table_schema = '{{ relation.schema }}'
      and table_name = '{{ relation.identifier }}'
    limit 1
  {% endcall %}

  {% set lookup = load_result('lookup_relation_kind') %}
  {% if lookup is not none and lookup['data'] is not none and (lookup['data'] | length) > 0 %}
    {% set kind = lookup['data'][0][0] %}
    {% if kind == 'view' %}
      {% call statement('drop_relation', auto_begin=False) %}
        drop view if exists {{ relation }}
      {% endcall %}
    {% elif kind == 'table' %}
      {% call statement('drop_relation', auto_begin=False) %}
        drop table if exists {{ relation }}
      {% endcall %}
    {% endif %}
  {% endif %}
{% endmacro %}
