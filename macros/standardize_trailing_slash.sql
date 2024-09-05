{% macro standardize_trailing_slash(url) %}
case
    when {{ url }} is null then '/'
    when RIGHT({{ url }},1) = '/' then {{ url }}
    else
        {{ url }} || '/'
end
{% endmacro %}