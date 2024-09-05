{%- macro unnest_key(column_to_unnest, key_to_extract, value_type = "string_value") -%}

(select value.{{value_type}} from unnest({{column_to_unnest}}) where key = '{{key_to_extract}}')

{%- endmacro -%}