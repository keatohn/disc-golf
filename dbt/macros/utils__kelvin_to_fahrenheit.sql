{% macro kelvin_to_fahrenheit(kelvin) %}
    {# Convert Kelvin to Fahrenheit #}
    cast(
        (({{ kelvin }} - 273.15) * 1.8) + 32
    as double)
{% endmacro %}

