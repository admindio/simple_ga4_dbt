{% macro clean_page_paths(url) %}
{{ return(adapter.dispatch('clean_page_paths', 'admind_dbt_ga4')(url)) }}
{% endmacro %}

{% macro default__clean_page_paths(url) %}
{{ standardize_trailing_slash(url) }}
{% endmacro %}
