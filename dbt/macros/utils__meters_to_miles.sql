{% macro meters_to_miles(meters) %}
    {# Convert meters to miles #}
    cast(
        {{ meters }} / 1609.0
    as double)
{% endmacro %}

