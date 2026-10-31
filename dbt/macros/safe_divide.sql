{# ==============================================================================
   Macro: safe_divide
   Performs division safely, returning null or a default value when dividing by zero.
   ============================================================================== #}

{% macro safe_divide(numerator, denominator, decimals=2, default_val='null') %}
  case
    when cast({{ denominator }} as numeric) is null or cast({{ denominator }} as numeric) = 0
    then {{ default_val }}
    else round(cast({{ numerator }} as numeric) / cast({{ denominator }} as numeric), {{ decimals }})
  end
{% endmacro %}
