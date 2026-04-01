{% test regex_match(model, column_name, regex) %}
with base as (
  select {{ column_name }} as value
  from {{ model }}
),
violations as (
  select *
  from base
  where value is not null
    and not regexp_like(cast(value as {{ dbt.type_string() }}), '{{ regex }}')
)
select *
from violations
{% endtest %}

