{# ==============================================================================
   Macro: convert_miles_to_km
   Converts miles to kilometres (standard factor: 1.60934)
   ============================================================================== #}

{% macro convert_miles_to_km(miles_column, decimals=3) %}
  round(cast({{ miles_column }} as numeric) * 1.60934, {{ decimals }})
{% endmacro %}
