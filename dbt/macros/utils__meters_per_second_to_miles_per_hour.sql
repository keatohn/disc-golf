{% macro meters_per_second_to_miles_per_hour(mps) %}
    {# Convert meters per second to miles per hour #}
    cast(
        {{ mps }} * 2.23694
    as double)
{% endmacro %}

