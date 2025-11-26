{% macro distance_feet(from_lat, from_lon, to_lat, to_lon) %}
    {# Calculate distance in feet between two coordinates #}
    {{ meters_to_feet(distance_meters(from_lat, from_lon, to_lat, to_lon)) }}
{% endmacro %}

