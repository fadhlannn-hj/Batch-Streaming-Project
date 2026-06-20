{% macro clean_string(column_name, default_value=none) %}

  {% if default_value %}
    COALESCE(UPPER(TRIM({{ column_name }})), '{{ default_value }}')
  {% else %}
    UPPER(TRIM({{ column_name }}))
  {% endif %}
  
{% endmacro %}