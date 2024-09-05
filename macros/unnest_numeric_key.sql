{%- macro unnest_numeric_key(column_to_unnest, key_to_extract) -%}

    (select coalesce(value.float_value, value.int_value) from unnest({{column_to_unnest}}) where key = '{{key_to_extract}}')

{%- endmacro -%}