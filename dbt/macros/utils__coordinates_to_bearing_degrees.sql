{% macro coordinates_to_bearing_degrees(from_lat, from_lon, to_lat, to_lon) %}
    {# Calculate bearing in degrees from one coordinate to another using haversine formula #}
    cast(
        mod(
            degrees(
                atan2(
                    sin(radians({{ to_lon }} - {{ from_lon }})) * cos(radians({{ to_lat }})),
                    cos(radians({{ from_lat }})) * sin(radians({{ to_lat }}))
                      - sin(radians({{ from_lat }})) * cos(radians({{ to_lat }})) * cos(radians({{ to_lon }} - {{ from_lon }}))
                )
            ) + 360,
            360
        )
    as double)
{% endmacro %}

