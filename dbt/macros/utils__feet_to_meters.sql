{% macro feet_to_meters(feet) %}
    {# Convert feet to meters #}
    cast({{ feet }} * 0.3048 as double)
{% endmacro %}

