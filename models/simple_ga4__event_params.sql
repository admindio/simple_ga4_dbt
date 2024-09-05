{{ config(materialized = 'view') }}
{# anything that select from this model can limit by "where date_tz >= 'yyyy-mm-dd'" to process less data #}

{% set NOT_SET = var("not_set_slug") %}
with unnest_stuff as (
    select
        id as event_id,
        event_timestamp_utc,
        session_id,
        event_name,
        user_pseudo_id,
        page_url,
        p.key,
        p.key as event_parameter_name,
        p.value.string_value as string_value,
        p.value.int_value as int_value,
        p.value.float_value as float_value,
        p.value.double_value as double_value,
        COALESCE(
            p.value.string_value,
            CAST(p.value.int_value AS STRING),
            CAST(p.value.float_value AS STRING),
            CAST(p.value.double_value AS STRING),
            '{{ NOT_SET }}'
        ) as event_parameter_value,
        date_tz
    from {{ ref("simple_ga4__events") }}, unnest(event_params) as p
)
select
    event_id || key as id,
    *
from unnest_stuff
