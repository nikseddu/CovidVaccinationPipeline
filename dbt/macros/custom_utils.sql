{% macro case_when(condition, true_value, false_value) %}
    CASE WHEN {{ condition }} THEN {{ true_value }} ELSE {{ false_value }} END
{% endmacro %}