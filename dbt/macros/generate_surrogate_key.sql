{# ==============================================================================
   Macro: generate_surrogate_key
   Role: Deterministic Surrogate Key Generation
   
   WHAT THIS MACRO DOES:
   Generates an MD5 surrogate hash key from a list of columns.
   Provides a built-in fallback when dbt_utils package is not installed.
   
   Example:
     {{ generate_surrogate_key(['trip_id']) }}
     {{ generate_surrogate_key(['pickup_lat', 'pickup_lng']) }}
   ============================================================================== #}

{% macro generate_surrogate_key(field_list) %}
  md5(
    {% for field in field_list %}
      coalesce(cast({{ field }} as varchar), '_null_')
      {% if not loop.last %} || '-' || {% endif %}
    {% endfor %}
  )
{% endmacro %}
