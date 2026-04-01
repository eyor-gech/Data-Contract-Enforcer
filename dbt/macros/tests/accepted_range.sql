{% test accepted_range(model, column_name, min_value=None, max_value=None, inclusive=True) %}
with base as (
  select {{ column_name }} as value
  from {{ model }}
),
violations as (
  select *
  from base
  where value is not null
    and (
      {% if min_value is not none %}
        {% if inclusive %} value < {{ min_value }} {% else %} value <= {{ min_value }} {% endif %}
      {% else %}
        false
      {% endif %}
      {% if max_value is not none %}
        or {% if inclusive %} value > {{ max_value }} {% else %} value >= {{ max_value }} {% endif %}
      {% else %}
        or false
      {% endif %}
    )
)
select *
from violations
{% endtest %}

