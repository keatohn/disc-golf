{% macro create_constraints() %}
  {# DuckDB-compatible constraint creation macro #}
  {# DuckDB supports PRIMARY KEY and UNIQUE constraints, but we'll use a no-op for analytics models #}
  {# Constraints are handled at the model level via unique_key config, not via post-hooks #}
  {% if execute %}
    {{ log("DuckDB constraints are handled via model unique_key configs, not post-hooks", info=True) }}
  {% endif %}
{% endmacro %}

