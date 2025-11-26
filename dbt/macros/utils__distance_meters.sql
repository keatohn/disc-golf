{% macro distance_meters(from_lat, from_lon, to_lat, to_lon) %}
    {# Calculate distance in meters between two coordinates using haversine formula #}
    {# DuckDB doesn't have ST_DISTANCE, so we use the haversine formula directly #}
    cast(
        6371000 * 2 * asin(
            sqrt(
                power(sin(radians(({{ to_lat }} - {{ from_lat }}) / 2)), 2) +
                cos(radians({{ from_lat }})) * cos(radians({{ to_lat }})) *
                power(sin(radians(({{ to_lon }} - {{ from_lon }}) / 2)), 2)
            )
        )
    as double)
{% endmacro %}

