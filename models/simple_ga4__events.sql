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

{% set session_length_seconds = 1800 %}
{% set NOT_SET = var("not_set_slug") %}
{% set DAG_MAX_DAYS = var("dag_max_days", none) %}

-- Fake out the source for nice docs{{ source("ga4", "events") }}
with
ga4_sources as (
{% for s in var("ga4_properties") %}
    -- {{ s["name"] }}: {{ s["dataset"] }}
    select
        event_name,
        event_timestamp,
        "{{ s.name }}" as business_unit,
        event_params,
        event_previous_timestamp,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_pseudo_id,
        privacy_info,
        user_properties,
        user_first_touch_timestamp,
        user_ltv,
        device,
        geo,
        app_info,
        traffic_source,
        collected_traffic_source,
        session_traffic_source_last_click,
        stream_id,
        platform,
        event_dimensions,
        ecommerce,
        items,
        CASE WHEN REGEXP_CONTAINS(_TABLE_SUFFIX, r'^intraday_') THEN 'streaming' ELSE 'daily' END AS is_streaming,
        parse_date('%Y%m%d', right(_table_suffix,8)) as date_tz
    from `{{ s["source_gcp_project"] }}.{{ s["dataset"] }}.events_*`
    where 1=1
    {% if target.name == "preview" or DAG_MAX_DAYS is not none %}
        -- Smaller dev datasets for fast iteration of Molly Data Enrichments
        and right(_TABLE_SUFFIX, 8) between
            FORMAT_TIMESTAMP('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL {{ DAG_MAX_DAYS if DAG_MAX_DAYS is not none and DAG_MAX_DAYS < var("PREVIEW_DATASET_DAYS", 0) else var("PREVIEW_DATASET_DAYS", 7) }} DAY))
            and
            FORMAT_TIMESTAMP('%Y%m%d', CURRENT_DATE())
    {% endif %}
    {% if is_incremental() %}
        --re-run given number of days every incremental run
        and right(_table_suffix,8) >= format_date('%Y%m%d', date_add(_dbt_max_partition, interval -{{var('incremental_refresh_days')}} day))
    {% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
),

-- create a user_pseudo_id when it comes through null; this should only happen when a user opts out of tracking or is blocking cookies
handle_null_user_pseudo_id_and_unnest_params as (
    select
        ifnull(user_pseudo_id, GENERATE_UUID()) as user_pseudo_id,
        * except(user_pseudo_id),
        case when user_pseudo_id is null then true else false end as has_null_user_pseudo_id,

        -----------------------------------------
        -- START: ATTRIBUTION FIELDS
        -----------------------------------------


        -- collected_traffic_source is present as of mid 2023
        -- there is session_traffic_source_last_click as of mid 2024
        -- Google ref: https://support.google.com/analytics/answer/7029846?hl=en#zippy=%2Ccollected-traffic-source%2Csession-traffic-source-last-click
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.source, '(not set)'),
            collected_traffic_source.manual_source,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_source') }},
            {{ unnest_key('event_params', 'source', 'string_value') }}
        ) as source,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.medium, '(not set)'),
            collected_traffic_source.manual_medium,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_medium') }},
            {{ unnest_key('event_params', 'medium', 'string_value') }}
        ) as medium,
        -- With campaign names/ids, prefer to the Google-sync'd ones for last session over manual ones for last session
        -- and both over manual ones on the event. This makes joining with cost data the most natural.
        COALESCE(
            nullif(session_traffic_source_last_click.google_ads_campaign.campaign_name, '(not set)'),
            nullif(session_traffic_source_last_click.manual_campaign.campaign_name, '(not set)'),
            collected_traffic_source.manual_campaign_name,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_campaign') }},
            {{ unnest_key('event_params', 'campaign', 'string_value') }}
        ) as campaign,
        COALESCE(
            nullif(session_traffic_source_last_click.google_ads_campaign.campaign_id, '(not set)'),
            nullif(session_traffic_source_last_click.manual_campaign.campaign_id, '(not set)'),
            collected_traffic_source.manual_campaign_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'campaign_id') }},
            {{ unnest_key('event_params', 'campaign_id', 'string_value') }}
        ) as campaign_id,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.content, '(not set)'),
            collected_traffic_source.manual_content,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_content') }},
            {{ unnest_key('event_params', 'content', 'string_value') }}
        ) as content,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.term, '(not set)'),
            collected_traffic_source.manual_term,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_term') }},
            {{ unnest_key('event_params', 'term', 'string_value') }}
        ) as term,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.creative_format, '(not set)'),
            collected_traffic_source.manual_creative_format
        ) as creative_format,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.marketing_tactic, '(not set)'),
            collected_traffic_source.manual_marketing_tactic
        ) as marketing_tactic,
        COALESCE(
            nullif(session_traffic_source_last_click.manual_campaign.source_platform, '(not set)'),
            collected_traffic_source.manual_source_platform
        ) as source_platform,

        -- Google click id
        coalesce(
            collected_traffic_source.gclid,
            {{ unnest_key('event_params', 'gclid', 'string_value') }},
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'gclid') }}
        ) as gclid,

        -- Get Google-recorded first traffic sources, untangle in sessions table later
        traffic_source.source as first_traffic_source_source,
        traffic_source.medium as first_traffic_source_medium,
        traffic_source.name as first_traffic_source_campaign,

        -- Tracking parameters from adMind standard template
        struct(
            COALESCE(
                nullif(session_traffic_source_last_click.google_ads_campaign.customer_id, '(not set)'),
                {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_acid') }}
            ) as google_ads_customer_id,
            nullif(session_traffic_source_last_click.google_ads_campaign.account_name, '(not set)') as google_ads_account_name,
            nullif(session_traffic_source_last_click.google_ads_campaign.ad_group_id, '(not set)') as google_ads_ad_group_id,
            COALESCE(
                nullif(session_traffic_source_last_click.google_ads_campaign.ad_group_name, '(not set)'),
                {{ extract_url_param(unnest_key('event_params', 'page_location'), 'utm_content') }}
            ) as google_ads_ad_group_name,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_mt') }} as google_ads_matchtype,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_adid') }} as google_ads_ad_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_exid') }} as google_ads_extension_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_dev') }} as google_ads_device,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_devm') }} as google_ads_device_model,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_nwk') }} as google_ads_network,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_locp') }} as google_ads_location_physical,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_loci') }} as google_ads_location_interest,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_pmt') }} as google_ads_placement,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_tgt') }} as google_ads_target,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_adt') }} as google_ads_adtype,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_sid') }} as google_ads_video_source_id
        ) as event_traffic_source_template_google_ads,
        struct(
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_mid') }} as google_ads_merchant_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_pch') }} as google_ads_product_channel,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_pid') }} as google_ads_product_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_pc') }} as google_ads_product_country,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_pl') }} as google_ads_product_language,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_ppid') }} as google_ads_product_partition_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_sc') }} as google_ads_store_code
        ) as event_traffic_source_template_google_pmax_shopping,
        struct(
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hcid') }} as google_ads_hotelcenter_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hid') }} as google_ads_hotel_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hpid') }} as google_ads_hotel_partition_id,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hatype') }} as google_ads_hotel_adtype,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hstd') }} as google_ads_travel_start_day,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hstm') }} as google_ads_travel_start_month,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hsty') }} as google_ads_travel_start_year,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hend') }} as google_ads_travel_end_day,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_henm') }} as google_ads_travel_end_month,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_heny') }} as google_ads_travel_end_year,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hadv') }} as google_ads_advanced_booking_window,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_dtp') }} as google_ads_date_type,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hnum') }} as google_ads_number_of_adults,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_htot') }} as google_ads_price_displayed_total,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_htx') }} as google_ads_price_displayed_tax,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hcur') }} as google_ads_user_currency,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hlg') }} as google_ads_user_language,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'g_hrtid') }} as google_ads_rate_rule_id
        ) as event_traffic_source_template_google_hotel,

        -- Other Click Parameters
        struct(
            COALESCE(
                collected_traffic_source.dclid,
                {{ extract_url_param(unnest_key('event_params', 'page_location'), 'dclid') }}
            ) as dclid,
            COALESCE(
                collected_traffic_source.srsltid,
                {{ extract_url_param(unnest_key('event_params', 'page_location'), 'srsltid') }}
            ) as srsltid,
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'msclkid') }} as msclkid, -- Microsoft Ads
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'fbclid') }} as fbclid,  -- Facebook
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'gbraid') }} as gbraid, -- Google App
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'wbraid') }} as wbraid, -- Google App
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'li_fat_id') }} as li_fat_id, -- LinkedIn
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'twclid') }} as twclid, -- Twitter (X)
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'rdt_cid') }} as rdt_cid, -- Reddit
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'ttclid') }} as ttclid, -- TikTok
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'ScCid') }} as ScCid, -- Snapchat
            {{ extract_url_param(unnest_key('event_params', 'page_location'), 'irclickid') }} as irclickid -- impact.com
        ) as event_traffic_source_click_ids,

        -----------------------------------------
        -- END: ATTRIBUTION FIELDS
        -----------------------------------------
        -- TODO handle nulls in referrer_hostname (if possible) (see https://github.com/admindio/issue-tracker/issues/219)
        regexp_extract({{ unnest_key('event_params', 'referrer', 'string_value') }}, r'^(?:(?:[^@:\/\s]+):\/?)?\/?([^@:\/\s\?]+)(?::(?:\d+))?(?:[^\?#]*)?\??(?:[^#]*)?#?(?:.+)?') as referrer_hostname,
        {{ unnest_key('event_params', 'page_title') }} as page_title,
        {{ unnest_key('event_params', 'page_location') }} as page_url,
        {{ unnest_key('event_params', 'page_referrer') }} as page_referrer,
        {{ unnest_key('event_params', 'referrer', 'string_value') }} as referrer,
        IFNULL({{ unnest_key('event_params', 'ignore_referrer', 'string_value') }}, 'false') as ignore_referrer,
        {{ unnest_key('event_params', 'ga_session_id', value_type = 'int_value') }} as ga_session_id,
        {{ unnest_key('event_params', 'ga_session_number', value_type = 'int_value') }} as ga_session_number,
        cast({{ unnest_key('event_params', 'engagement_time_msec') }} as int64)/ 1000 as engagement_time_sec,
    from ga4_sources
),

fix_event_attribution as (
    -- This logic is duplicated in the first touch / streaming fix CTE
    select
        *,
        case
            -- override medium and source when gclid (or wbraid or gbraid) is present
            -- Refer to this: https://www.semetis.com/en/resources/articles/understanding-gbraid-the-difference-with-wbraid-and-gclid
            when coalesce(gclid, event_traffic_source_click_ids.wbraid, event_traffic_source_click_ids.gbraid) is not null
                then struct(
                            ifnull(campaign, '{{ NOT_SET }}') as campaign,
                            'cpc' as medium,
                            'google' as source,
                            coalesce(content, '{{ NOT_SET }}') as content,
                            coalesce(term, '{{ NOT_SET }}') as term,
                            coalesce(campaign_id, '{{ NOT_SET }}') as campaign_id,
                            coalesce(referrer, '{{ NOT_SET }}') as referrer,
                            gclid as gclid
                    )
            -- if any of the source/medium/campaign are present and it isn't a referral, use the s/m/c
            when coalesce(source, medium, campaign) is not null and not (medium = 'referral' and ignore_referrer = 'true')
                then struct(
                        ifnull(campaign, '{{ NOT_SET }}')  as campaign,
                        ifnull(medium, '{{ NOT_SET }}') as medium,
                        ifnull(source, '{{ NOT_SET }}') as source,
                        coalesce(content, '{{ NOT_SET }}') as content,
                        coalesce(term, '{{ NOT_SET }}') as term,
                        coalesce(campaign_id, '{{ NOT_SET }}') as campaign_id,
                        coalesce(referrer, '{{ NOT_SET }}') as referrer,
                        gclid as gclid
                )
            -- if all of source/medium/campaign are null and referrer is not null then fill in referral payload.
            when referrer is not null and not ignore_referrer = 'true'
                then struct(
                        '{{ NOT_SET }}' as campaign,
                        'referral' as medium,
                        ifnull(referrer_hostname, '{{ NOT_SET }}') as source,
                        '{{ NOT_SET }}' as content,
                        '{{ NOT_SET }}' as term,
                        '{{ NOT_SET }}' as campaign_id,
                        coalesce(referrer, '{{ NOT_SET }}') as referrer,
                        gclid as gclid
                )
            else
                struct(
                    '{{ NOT_SET }}' as campaign,
                    '(none)' as medium,
                    '(direct)' as source,
                    '{{ NOT_SET }}' as content,
                    '{{ NOT_SET }}' as term,
                    '{{ NOT_SET }}' as campaign_id,
                    coalesce(referrer, '{{ NOT_SET }}') as referrer,
                    gclid as gclid
                )
        end as event_traffic_source
    from handle_null_user_pseudo_id_and_unnest_params
),

standardize_column_names as (
    select
        event_name,
        event_timestamp as event_timestamp_utc,
        business_unit,
        event_params,
        event_previous_timestamp as event_previous_timestamp_utc,
        event_value_in_usd,
        event_bundle_sequence_id,
        event_server_timestamp_offset,
        user_id,
        user_properties,
        user_pseudo_id,
        has_null_user_pseudo_id,
        ga_session_id,
        ga_session_number,
        privacy_info.analytics_storage as privacy_info_analytics_storage,
        privacy_info.ads_storage as privacy_info_ads_storage,
        privacy_info.uses_transient_token as privacy_info_uses_transient_token,
        device.category as device_category,
        device.mobile_brand_name as device_brand,
        device.mobile_model_name as device_mobile_model_name,
        device.mobile_os_hardware_model as device_mobile_os_hardware_model,
        device.mobile_marketing_name as device_mobile_marketing_name,
        device.operating_system as device_operating_system,
        device.operating_system_version as device_operating_system_version,
        device.language as device_language,
        device.is_limited_ad_tracking as device_is_limited_ad_tracking,
        device.advertising_id as device_advertising_id,
        device.vendor_id as device_vendor_id,
        coalesce(device.browser, device.web_info.browser) as device_browser,
        coalesce(device.browser_version, device.web_info.browser_version) as device_browser_version,
        device.web_info.hostname as hostname,
        geo.continent as geo_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city,
        geo.metro as geo_metro,
        geo.sub_continent as geo_sub_continent,
        ecommerce.total_item_quantity as ecommerce_total_item_quantity,
        ecommerce.purchase_revenue_in_usd as ecommerce_purchase_revenue_in_usd,
        ecommerce.purchase_revenue as ecommerce_purchase_revenue,
        ecommerce.refund_value_in_usd as ecommerce_refund_value_in_usd,
        ecommerce.refund_value as ecommerce_refund_value,
        ecommerce.shipping_value_in_usd as ecommerce_shipping_value_in_usd,
        ecommerce.shipping_value as ecommerce_shipping_value,
        ecommerce.tax_value_in_usd as ecommerce_tax_value_in_usd,
        ecommerce.tax_value as ecommerce_tax_value,
        ecommerce.unique_items as ecommerce_unique_items,
        ecommerce.transaction_id as ecommerce_transaction_id,
        items,
        stream_id,
        platform,
        event_traffic_source,
        event_traffic_source_template_google_ads,
        event_traffic_source_template_google_pmax_shopping,
        event_traffic_source_template_google_hotel,
        event_traffic_source_click_ids,
        app_info.id as app_info_id,
        page_title,
        page_url,
        gclid,
        page_referrer,
        referrer,
        referrer_hostname,
        ignore_referrer,
        engagement_time_sec,
        is_streaming,
        date_tz
    from fix_event_attribution
)

, pre_event_id_order_and_rank as (
    select
        *,
        row_number() over (
            partition by user_pseudo_id
            order by event_timestamp_utc asc, event_name asc
        ) as row_num
    from standardize_column_names
)

-- build the "primary key"
, create_event_id as (
    select
        {{ generate_surrogate_key(['stream_id', 'user_pseudo_id', 'event_timestamp_utc', 'row_num']) }} as event_id,
        * except(row_num)
    from pre_event_id_order_and_rank
)

, handle_null_ga_session_id as (
    select coalesce(ga_session_id,
            last_value(ga_session_id ignore nulls)
                over (partition by user_pseudo_id
                        order by unix_seconds(timestamp_micros(event_timestamp_utc)) asc
                            range between {{ session_length_seconds }} preceding and {{ session_length_seconds }} following)
                    , cast(left(cast(event_timestamp_utc as string),10) as int)
            ) as ga_session_id,
    * except(ga_session_id)
    from create_event_id
)

, add_unique_session_id_and_timestamp as (
    select
        {{ generate_surrogate_key(['user_pseudo_id', 'ga_session_id']) }} as session_id,
        * except(event_timestamp_utc),
        timestamp_micros(event_timestamp_utc) as event_timestamp_utc,
    from handle_null_ga_session_id
),

parse_url_parts_to_columns as (
    select
        * except(hostname),
        regexp_extract(page_url,
            r'^(?:([^@:\/\s]+):\/?)?\/?(?:[^@:\/\s\?]+)(?::(?:\d+))?(?:[^\?#]*)?\??(?:[^#]*)?#?(?:.+)?'
        ) as page_protocol,
        hostname as page_hostname,
        regexp_extract(page_url,
            r'^(?:(?:[^@:\/\s]+):\/?)?\/?(?:[^@:\/\s\?]+)(?::(\d+))?(?:[^\?#]*)?\??(?:[^#]*)?#?(?:.+)?') as page_port,
        hostname || {{ clean_page_paths( "regexp_extract(page_url, r'^(?:(?:[^@:\/\s]+):\/?)?\/?(?:[^@:\/\s\?]+)(?::(?:\d+))?([^\?#]*)?\??(?:[^#]*)?#?(?:.+)?')" ) }} as page_path,
        regexp_extract(page_url,
            r'^(?:(?:[^@:\/\s]+):\/?)?\/?(?:[^@:\/\s\?]+)(?::(?:\d+))?(?:[^\?#]*)?\??([^#]*)?#?(?:.+)?') as page_query,
        regexp_extract(page_url,
            r'^(?:(?:[^@:\/\s]+):\/?)?\/?(?:[^@:\/\s\?]+)(?::(?:\d+))?(?:[^\?#]*)?\??(?:[^#]*)?#?(.+)?'
        ) as page_fragment,
    from add_unique_session_id_and_timestamp
),

previous_next_page_path as (
    select
        event_id,
        timestamp_diff(lead(event_timestamp_utc) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc), event_timestamp_utc, SECOND) as seconds_on_page,
        row_number() over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) = 1 as is_entrance,
        row_number() over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc desc) = 1 as is_exit,
        row_number() over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as page_sequence,
        ifnull(lag(page_path, 2) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc), 'Entrance') as prev_prev_page_path,
        lag(event_id) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as previous_page_event_id,
        lag(event_timestamp_utc) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as previous_page_ts,
        ifnull(lag(page_path) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc), 'Entrance') as previous_page_path,
        ifnull(lead(page_path) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc), 'Exit') as next_page_path,
        lead(event_id) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as next_page_event_id,
        lead(event_timestamp_utc) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as next_page_ts,
        ifnull(lead(page_path, 2) over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc), 'Exit') as next_next_page_path,
    from parse_url_parts_to_columns
    where event_name = 'page_view'
)

, event_sequence as (
    select
        event_id,
        row_number() over (partition by user_pseudo_id, ga_session_id order by event_timestamp_utc asc) as event_sequence,
    from parse_url_parts_to_columns
    where 1=1
        and event_name not in('session_start', 'first_visit')
)

, reorder as (
select
    e.event_id as id,
    e.event_timestamp_utc,
    e.date_tz,
    EXTRACT(HOUR FROM e.event_timestamp_utc) as event_hour_of_day_utc,
    e.business_unit,
    e.session_id,
    e.ga_session_id,
    e.ga_session_number,
    e.user_id,
    e.user_pseudo_id,
    e.event_name,
    es.event_sequence,
    e.event_params,
    e.event_previous_timestamp_utc,
    e.event_value_in_usd,
    e.event_bundle_sequence_id,
    e.event_server_timestamp_offset,
    e.user_properties,
    e.has_null_user_pseudo_id,
    e.privacy_info_analytics_storage,
    e.privacy_info_ads_storage,
    e.privacy_info_uses_transient_token,
    e.device_category,
    e.device_brand,
    e.device_mobile_model_name,
    e.device_mobile_os_hardware_model,
    e.device_mobile_marketing_name,
    e.device_operating_system,
    e.device_operating_system_version,
    e.device_language,
    e.device_is_limited_ad_tracking,
    e.device_advertising_id,
    e.device_vendor_id,
    e.device_browser,
    e.device_browser_version,
    e.geo_continent,
    e.geo_country,
    e.geo_region,
    e.geo_city,
    e.geo_metro,
    e.geo_sub_continent,
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
    e.items,
    e.stream_id,
    e.platform,
    e.event_traffic_source,
    e.event_traffic_source_template_google_ads,
    e.event_traffic_source_template_google_pmax_shopping,
    e.event_traffic_source_template_google_hotel,
    e.event_traffic_source_click_ids,
    e.app_info_id,
    e.page_title,
    e.page_url,
    e.referrer,
    e.page_referrer,
    e.page_protocol,
    e.page_hostname,
    e.page_port,
    e.page_path,
    e.page_query,
    e.page_fragment,
    -- 2024-08-29: eventually we should get to page and page_path, right now it causes a Looker Studio pain that we wanna dodge.
    -- See https://github.com/admindio/issue-tracker/issues/224
    -- e.page_hostname || e.page_path as page,
    -- Leave this in place (it is a duplicate) just to not break any existing reports
    e.page_path as page,
    e.engagement_time_sec,
    p.seconds_on_page,
    p.is_entrance,
    p.is_exit,
    p.page_sequence,
    p.previous_page_path,
    p.previous_page_event_id,
    p.previous_page_ts as previous_page_ts_utc,
    p.prev_prev_page_path,
    p.next_page_path,
    p.next_page_event_id,
    p.next_page_ts as next_page_ts_utc,
    p.next_next_page_path,
    is_streaming,
from parse_url_parts_to_columns e
left join previous_next_page_path p on e.event_id = p.event_id
left join event_sequence es on es.event_id = e.event_id
)
select * from reorder
