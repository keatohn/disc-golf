{% macro meters_to_feet(meters) %}
    {# Convert meters to feet #}
    cast(
        {{ meters }} * 3.28084
    as double)
{% endmacro %}

