{% macro generate_erd() %}
-- ERD Generation for Disc Golf DBT Project
-- This macro generates a text-based representation of your data model
-- that can be used with ERD tools like dbdiagram.io or Lucidchart

{% set models = graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list %}

-- Tables and Columns
{% for model in models %}
-- Table: {{ model.name }}
{% if model.description %}
-- Description: {{ model.description }}
{% endif %}
{% for column in model.columns.values() %}
--   {{ column.name }}: {{ column.data_type }}
{% if column.description %}
--     {{ column.description }}
{% endif %}
{% endfor %}
{% if not loop.last %}

{% endif %}
{% endfor %}

-- Relationships (based on dbt tests)
{% for model in models %}
{% if model.tests %}
-- {{ model.name }} relationships:
{% for test_name, test_config in model.tests.items() %}
{% if test_name == 'relationships' %}
{% for relationship in test_config %}
--   {{ model.name }}.{{ relationship.field }} -> {{ relationship.to }}
{% endfor %}
{% endif %}
{% endfor %}
{% endif %}
{% endfor %}

-- Usage Instructions:
-- 1. Copy this output to dbdiagram.io
-- 2. Or use with Lucidchart/Draw.io
-- 3. This shows logical relationships without FK constraints
{% endmacro %} 
