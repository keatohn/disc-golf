{% macro feet_to_miles(feet) %}
    {# Convert feet to miles #}
    cast({{ feet }} / 5280.0 as double)
{% endmacro %}

