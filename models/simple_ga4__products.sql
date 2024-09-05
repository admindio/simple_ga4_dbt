with everything_but_id as (
    -- Note this is not actually products grain. It is event-item grain for all ecommerce events.
{# anything that select from this model can limit by "where date_tz >= 'yyyy-mm-dd'" to process less data #}
{{
    config(
        materialized='incremental',
        partition_by={
          "field": "date_tz",
          "data_type": "date",
          "copy_partitions": true
        },
        on_schema_change='sync_all_columns',
        incremental_strategy='insert_overwrite'
    )
}}
{% set NOT_SET = var("not_set_slug") %}
-- Note this is not actually products grain. It is event-item grain for all ecommerce events.
select
    e.id as event_id,
    e.event_timestamp_utc,
    CAST(e.event_timestamp_utc AS DATE) as event_date_utc,
    e.event_name,
    e.session_id,
    e.user_id,
    e.user_pseudo_id,

    -- event-level ecommerce
    e.ecommerce_total_item_quantity,
    e.ecommerce_purchase_revenue_in_usd,
    e.ecommerce_purchase_revenue,
    e.ecommerce_refund_value_in_usd,
    e.ecommerce_refund_value,
    e.ecommerce_shipping_value_in_usd,
    e.ecommerce_shipping_value,
    e.ecommerce_tax_value_in_usd,
    e.ecommerce_tax_value,
    e.ecommerce_unique_items,
    e.ecommerce_transaction_id,
    e.date_tz,

    -- item-level ecommerce
    ifnull(i.item_id, '{{ NOT_SET }}') as item_id,
    ifnull(i.item_name, '{{ NOT_SET }}') as item_name,
    ifnull(i.item_brand, '{{ NOT_SET }}') as item_brand,
    ifnull(i.item_variant, '{{ NOT_SET }}') as item_variant,
    ifnull(i.item_category, '{{ NOT_SET }}') as item_category,
    ifnull(i.item_category2, '{{ NOT_SET }}') as item_category2,
    ifnull(i.item_category3, '{{ NOT_SET }}') as item_category3,
    ifnull(i.item_category4, '{{ NOT_SET }}') as item_category4,
    ifnull(i.item_category5, '{{ NOT_SET }}') as item_category5,
    ifnull(i.coupon, '{{ NOT_SET }}') as coupon,
    ifnull(i.affiliation, '{{ NOT_SET }}') as affiliation,
    ifnull(i.location_id, '{{ NOT_SET }}') as location_id,
    ifnull(i.item_list_id, '{{ NOT_SET }}') as item_list_id,
    ifnull(i.item_list_name, '{{ NOT_SET }}') as item_list_name,
    ifnull(i.item_list_index, '{{ NOT_SET }}') as item_list_index,
    ifnull(i.promotion_id, '{{ NOT_SET }}') as promotion_id,
    ifnull(i.promotion_name, '{{ NOT_SET }}') as promotion_name,
    ifnull(i.creative_name, '{{ NOT_SET }}') as creative_name,
    ifnull(i.creative_slot, '{{ NOT_SET }}') as creative_slot,

    i.item_revenue_in_usd,
    i.quantity,
    i.price_in_usd,
    i.price,


    {%- set ecom_events = [
        "add_payment_info",
        "add_shipping_info",
        "add_to_cart",
        "add_to_wishlist",
        "begin_checkout",
        "purchase",
        "refund",
        "remove_from_cart",
        "select_item",
        "select_promotion",
        "view_cart",
        "view_item",
        "view_item_list",
        "view_promotion",
        ]
    -%}

    -- value metrics
{#    sum(case when event_name = 'purchase' then i.item_revenue_in_usd else 0 end) as item_revenue_in_usd,#}
{#    sum(case when event_name = 'purchase' then i.quantity else 0 end) as item_quantity,#}
{#    sum(case when event_name = 'purchase' then e.ecommerce_purchase_revenue_in_usd else 0 end) as purchase_revenue_in_usd,#}
{#    sum(case when event_name = 'purchase' then e.ecommerce_shipping_value_in_usd else 0 end) as shipping_value_in_usd,#}
{#    sum(case when event_name = 'purchase' then e.ecommerce_tax_value_in_usd else 0 end) as tax_value_in_usd,#}
{#    sum(case when event_name = 'refund' then i.item_refund_in_usd else 0 end) as item_refund_in_usd,#}
from {{ ref("simple_ga4__events") }} e, unnest(items) as i
where 1=1
    {% if is_incremental() %}
        --re-run given number of days every incremental run
        and e.date_tz >= date_add(_dbt_max_partition, interval -{{var('incremental_refresh_days')}} day)
    {% endif %}
    and event_name in ({% for event in ecom_events %}"{{ event }}"{% if not loop.last %},{% endif %}{% endfor %})
    -- TODO is this item_id null check needed?
{#    and i.item_id is not null#}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42
)

-- build the "primary key"
, create_event_specific_item_ids as (
    select
        {{ generate_surrogate_key(['event_id', 'item_id', 'item_name']) }} as event_specific_item_id,
        {{ generate_surrogate_key(['event_id', 'item_id', 'item_name', 'item_variant']) }} as event_specific_variant_id,
        *,
    from everything_but_id
)

select *
from create_event_specific_item_ids

