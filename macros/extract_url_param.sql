{%- macro extract_url_param(url, param) -%}

REGEXP_EXTRACT({{ url }}, r'{{ param }}=([^&#]+)')

{%- endmacro -%}