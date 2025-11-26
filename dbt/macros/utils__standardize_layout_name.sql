{% macro standardize_layout_name(layout_name) %}
    {# Standardize layout names to common values #}
    case
        when {{ layout_name }} ilike '%main%' then 'Main'
        when {{ layout_name }} ilike '%white%' then 'White'
        when {{ layout_name }} ilike '%blue%' then 'Blue'
        when {{ layout_name }} ilike '%red%' then 'Red'
        when {{ layout_name }} ilike '%black%' then 'Black'
        when {{ layout_name }} ilike '%green%' then 'Green'
        when {{ layout_name }} ilike '%gold%' then 'Gold'
        when {{ layout_name }} ilike '%long%' then 'Long'
        when {{ layout_name }} ilike '%short%' then 'Short'
        when {{ layout_name }} ilike '%front%' then 'Front 9'
        when {{ layout_name }} ilike '%back%' then 'Back 9'
        when {{ layout_name }} ilike '%league%' then 'League'
        when {{ layout_name }} ilike '%hybrid%' then 'Hybrid'
        when {{ layout_name }} ilike '%championship%' then 'Championship'
        when {{ layout_name }} ilike '%pro%' then 'Pro'
        when {{ layout_name }} ilike '%object%' then 'Object'
        when {{ layout_name }} ilike '%current%'
            or {{ layout_name }} like '%18%'
            or {{ layout_name }} ilike '%permanent%'
            or {{ layout_name }} ilike '%course%'
            then 'Main'
        when {{ layout_name }} like '%9%' then '9'
        when {{ layout_name }} like '%OG %'
            or {{ layout_name }} like '% OG%'
            or {{ layout_name }} ilike '%original%'
            then 'Original'
        when {{ layout_name }} ilike '%custom%' then 'Custom'
        else 'Other'
    end
{% endmacro %}

