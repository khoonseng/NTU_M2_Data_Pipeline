{% macro generate_date_key(column_name) %}
    {# Detect the database type and return the correct formatting function #}
    {% if target.name == 'cloud' %}
        -- cast(format_timestamp('%Y%m%d', {{ column_name }}) as INT64)
        DATE({{ column_name }})
    {% elif target.name == 'local' %}
        -- cast(strftime({{ column_name }}, '%Y%m%d') as INTEGER)
        CAST({{ column_name }} as DATE)
    {% else %}
        {{ column_name }} {# Fallback if target is neither #}
    {% endif %}
{% endmacro %}