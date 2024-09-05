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
{% set LAST_NON_DIRECT_LOOKBACK = var('last_non_direct_lookback', none) %}
with events as (
    select
        id,
        session_id,
        ga_session_id,
        event_timestamp_utc,
        event_name,
        date_tz,
        user_pseudo_id as user_pseudo_id,
        business_unit as business_unit,
        event_traffic_source,
        event_traffic_source_click_ids,
        event_traffic_source_template_google_ads,
        event_traffic_source_template_google_hotel,
        event_traffic_source_template_google_pmax_shopping,
        ifnull(page_referrer, '{{ NOT_SET }}') as landing_page_referrer,
        ifnull(page_hostname, '{{ NOT_SET }}') as landing_page_hostname,
        ifnull(page_path, '{{ NOT_SET }}') as landing_page,
        ifnull(page_query, '{{ NOT_SET }}') as landing_page_query,
        ifnull(device_category, '{{ NOT_SET }}') as device_category,
        ifnull(device_brand, '{{ NOT_SET }}') as device_brand,
        ifnull(device_operating_system, '{{ NOT_SET }}') as device_os,
        ifnull(device_operating_system_version, '{{ NOT_SET }}') as device_os_version,
        ifnull(device_browser, '{{ NOT_SET }}') as device_browser,
        ifnull(device_browser_version, '{{ NOT_SET }}') as device_browser_version,
        ifnull(geo_continent, '{{ NOT_SET }}') as geo_continent,
        ifnull(geo_sub_continent, '{{ NOT_SET }}') as geo_sub_continent,
        ifnull(geo_country, '{{ NOT_SET }}') as geo_country,
        ifnull(geo_region, '{{ NOT_SET }}') as geo_region,
        ifnull(geo_city, '{{ NOT_SET }}') as geo_city,
        ifnull(geo_metro, '{{ NOT_SET }}') as geo_metro,
        ga_session_number as session_number,

        referrer,

        event_params,

        case when ga_session_number = 1 then 'New' else 'Returning' end as user_type,
    from {{ ref('events_') }}
    where 1=1
    {% if is_incremental() %}
        --re-run given number of days every incremental run
        and date_tz >= date_add(_dbt_max_partition, interval -{{var('incremental_refresh_days')}} day)
    {% endif %}
),
sessions as (
    select
        session_id as id,
        event_timestamp_utc as session_start_at_utc,
        date_tz,
        CAST(event_timestamp_utc AS DATE) AS session_start_on_utc,
        user_pseudo_id,
        business_unit,
        event_traffic_source,
        event_traffic_source_click_ids,
        event_traffic_source_template_google_ads,
        event_traffic_source_template_google_hotel,
        event_traffic_source_template_google_pmax_shopping,
        landing_page_referrer,
        landing_page_hostname,
        landing_page,
        landing_page_query,
        device_category,
        device_brand,
        device_os,
        device_os_version,
        device_browser,
        device_browser_version,
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        geo_city,
        geo_metro,
        session_number,
        user_type,
    from events
    where event_name = "session_start"

    -- We have seen scenarios where there is more than one session_start event with the same ga_session_id in the raw data
    -- Take the first one by time, then prefer by most attribution info
    qualify row_number() over(
        partition by session_id
        order by
            event_timestamp_utc asc,
            -- If conflict at the same timestamp, prefer anything not referral or direct => referral => direct
            case
                when event_traffic_source.medium = '(none)' then 1
                when event_traffic_source.medium = 'referral' then 2
                else 3
            end desc
    ) = 1
),

find_session_attribution as (
    select
        session_id,
        event_timestamp_utc as session_start_at_utc,
        ga_session_id,
        user_pseudo_id,
        landing_page,
        landing_page_query,
        event_traffic_source,
        event_traffic_source_click_ids,
        event_traffic_source_template_google_ads,
        event_traffic_source_template_google_hotel,
        event_traffic_source_template_google_pmax_shopping,
        referrer,
    from events
    where 1=1
        -- TODO some say these two 'synthetic' events (session_start and first_visit) are not great for attribution. Investigate.
        -- and event_name not in ("session_start", "first_visit")
    qualify row_number() over (
        partition by session_id
        order by
            event_timestamp_utc asc,
            -- If conflict at the same timestamp, prefer anything not referral or direct => referral => direct
            case
                when event_traffic_source.medium = '(none)' then 1
                when event_traffic_source.medium = 'referral' then 2
                else 3
            end desc
    ) = 1
),

engagement_metrics as (
    select
        session_id,
        user_pseudo_id,
        ga_session_id,
        max((select ifnull(greatest(ifnull(safe_cast(value.string_value as int64),0), ifnull(value.int_value,0)),0) from unnest(event_params) where key = 'session_engaged')) as is_engaged,
        sum((select value.int_value from unnest(event_params) where key = 'engagement_time_msec'))/1000 as engaged_seconds,
        count(distinct concat(id, user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id'))) as event_count,
        max(event_timestamp_utc) as last_event_at_utc
    from events
    group by session_id, user_pseudo_id, ga_session_id
),

page_views as (
    select
        session_id,
        COUNT(*) AS page_views,
    from events
    where event_name = "page_view"
    group by session_id
),
find_last_non_direct_session_id as (
    select
        session_id,
        IFNULL(
            LAST_VALUE(IF(event_traffic_source.source = '(direct)', NULL, session_id) IGNORE NULLS)
                OVER (PARTITION BY user_pseudo_id ORDER BY session_start_at_utc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
            session_id
        ) as last_non_direct_session_id,
    from find_session_attribution
)

select
    s.* except(
        event_traffic_source,
        event_traffic_source_click_ids,
        event_traffic_source_template_google_ads,
        event_traffic_source_template_google_hotel,
        event_traffic_source_template_google_pmax_shopping
        ),



    -- TODO: this is a duplicate to landing_page. @Taylor do you want this?
    s.landing_page as last_landing_page,
    s.landing_page_query as last_landing_page_query,

    ------------------------------------------------------------
    -- START Last Attribution
    ------------------------------------------------------------
    lst_atr.event_traffic_source.source as last_source,
    lst_atr.event_traffic_source.medium as last_medium,
    lst_atr.event_traffic_source.source || ' / ' || lst_atr.event_traffic_source.medium as last_source_medium,
    lst_atr.event_traffic_source.campaign as last_campaign,
    lst_atr.event_traffic_source.content as last_content,
    lst_atr.event_traffic_source.term as last_term,
    lst_atr.event_traffic_source.campaign_id as last_campaign_id,
    lst_atr.event_traffic_source.referrer as last_referrer,
    lst_atr.event_traffic_source.gclid as last_gclid,

    -- Flatten structs
    lst_atr.event_traffic_source_template_google_ads.google_ads_customer_id as last_google_ads_customer_id,
    lst_atr.event_traffic_source_template_google_ads.google_ads_account_name as last_google_ads_account_name,
    lst_atr.event_traffic_source_template_google_ads.google_ads_ad_group_id as last_google_ads_ad_group_id,
    lst_atr.event_traffic_source_template_google_ads.google_ads_ad_group_name as last_google_ads_ad_group_name,
    lst_atr.event_traffic_source_template_google_ads.google_ads_matchtype as last_google_ads_matchtype,
    lst_atr.event_traffic_source_template_google_ads.google_ads_ad_id as last_google_ads_ad_id,
    lst_atr.event_traffic_source_template_google_ads.google_ads_extension_id as last_google_ads_extension_id,
    lst_atr.event_traffic_source_template_google_ads.google_ads_device as last_google_ads_device,
    lst_atr.event_traffic_source_template_google_ads.google_ads_device_model as last_google_ads_device_model,
    lst_atr.event_traffic_source_template_google_ads.google_ads_network as last_google_ads_network,
    lst_atr.event_traffic_source_template_google_ads.google_ads_location_physical as last_google_ads_location_physical,
    lst_atr.event_traffic_source_template_google_ads.google_ads_location_interest as last_google_ads_location_interest,
    lst_atr.event_traffic_source_template_google_ads.google_ads_placement as last_google_ads_placement,
    lst_atr.event_traffic_source_template_google_ads.google_ads_target as last_google_ads_target,
    lst_atr.event_traffic_source_template_google_ads.google_ads_adtype as last_google_ads_adtype,
    lst_atr.event_traffic_source_template_google_ads.google_ads_video_source_id as last_google_ads_video_source_id,

    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_merchant_id as last_google_ads_merchant_id,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_product_channel as last_google_ads_product_channel,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_product_id as last_google_ads_product_id,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_product_country as last_google_ads_product_country,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_product_language as last_google_ads_product_language,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_product_partition_id as last_google_ads_product_partition_id,
    lst_atr.event_traffic_source_template_google_pmax_shopping.google_ads_store_code as last_google_ads_store_code,

    -- TODO: some sort of switch for this?
    -- Google Ads Hotel (stuff)
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_hotelcenter_id as last_google_ads_hotelcenter_id,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_hotel_id as last_google_ads_hotel_id,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_hotel_partition_id as last_google_ads_hotel_partition_id,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_hotel_adtype as last_google_ads_hotel_adtype,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_start_day as last_google_ads_travel_start_day,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_start_month as last_google_ads_travel_start_month,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_start_year as last_google_ads_travel_start_year,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_end_day as last_google_ads_travel_end_day,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_end_month as last_google_ads_travel_end_month,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_travel_end_year as last_google_ads_travel_end_year,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_advanced_booking_window as last_google_ads_advanced_booking_window,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_date_type as last_google_ads_date_type,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_number_of_adults as last_google_ads_number_of_adults,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_price_displayed_total as last_google_ads_price_displayed_total,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_price_displayed_tax as last_google_ads_price_displayed_tax,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_user_currency as last_google_ads_user_currency,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_user_language as last_google_ads_user_language,#}
{#    lst_atr.event_traffic_source_template_google_hotel.google_ads_rate_rule_id as last_google_ads_rate_rule_id,#}

    -- Click ids
    lst_atr.event_traffic_source_click_ids.dclid as last_dclid,
    lst_atr.event_traffic_source_click_ids.srsltid as last_srsltid,
    lst_atr.event_traffic_source_click_ids.msclkid as last_msclkid,
    lst_atr.event_traffic_source_click_ids.fbclid as last_fbclid,
    lst_atr.event_traffic_source_click_ids.gbraid as last_gbraid,
    lst_atr.event_traffic_source_click_ids.wbraid as last_wbraid,
    lst_atr.event_traffic_source_click_ids.li_fat_id as last_li_fat_id,
    lst_atr.event_traffic_source_click_ids.twclid as last_twclid,
    lst_atr.event_traffic_source_click_ids.rdt_cid as last_rdt_cid,
    lst_atr.event_traffic_source_click_ids.ttclid as last_ttclid,
    lst_atr.event_traffic_source_click_ids.ScCid as last_ScCid,
    lst_atr.event_traffic_source_click_ids.irclickid as last_irclickid,

    ------------------------------------------------------------
    -- END Last Attribution
    ------------------------------------------------------------


    ------------------------------------------------------------
    -- START Last Non-Direct Attribution
    ------------------------------------------------------------
    case when lnd_session.id is null then s.landing_page else lnd_session.landing_page end as last_non_direct_landing_page,
    case when lnd_session.id is null then s.landing_page_query else lnd_session.landing_page_query end as last_non_direct_landing_page_query,

    case when lnd_session.id is null then s.event_traffic_source.source else lnd_session.event_traffic_source.source end as last_non_direct_source,
    case when lnd_session.id is null then s.event_traffic_source.medium else lnd_session.event_traffic_source.medium end as last_non_direct_medium,
    case when lnd_session.id is null then s.event_traffic_source.source || ' / ' || s.event_traffic_source.medium else lnd_session.event_traffic_source.source || ' / ' || lnd_session.event_traffic_source.medium end as last_non_direct_source_medium,
    case when lnd_session.id is null then s.event_traffic_source.campaign else lnd_session.event_traffic_source.campaign end as last_non_direct_campaign,
    case when lnd_session.id is null then s.event_traffic_source.content else lnd_session.event_traffic_source.content end as last_non_direct_content,
    case when lnd_session.id is null then s.event_traffic_source.term else lnd_session.event_traffic_source.term end as last_non_direct_term,
    case when lnd_session.id is null then s.event_traffic_source.campaign_id else lnd_session.event_traffic_source.campaign_id end as last_non_direct_campaign_id,
    case when lnd_session.id is null then s.event_traffic_source.referrer else lnd_session.event_traffic_source.referrer end as last_non_direct_referrer,
    case when lnd_session.id is null then s.event_traffic_source.gclid else lnd_session.event_traffic_source.gclid end as last_non_direct_gclid,

    -- Flatten structs
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_customer_id else lnd_session.event_traffic_source_template_google_ads.google_ads_customer_id end as last_non_direct_google_ads_customer_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_account_name else lnd_session.event_traffic_source_template_google_ads.google_ads_account_name end as last_non_direct_google_ads_account_name,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_ad_group_id else lnd_session.event_traffic_source_template_google_ads.google_ads_ad_group_id end as last_non_direct_google_ads_ad_group_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_ad_group_name else lnd_session.event_traffic_source_template_google_ads.google_ads_ad_group_name end as last_non_direct_google_ads_ad_group_name,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_matchtype else lnd_session.event_traffic_source_template_google_ads.google_ads_matchtype end as last_non_direct_google_ads_matchtype,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_ad_id else lnd_session.event_traffic_source_template_google_ads.google_ads_ad_id end as last_non_direct_google_ads_ad_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_extension_id else lnd_session.event_traffic_source_template_google_ads.google_ads_extension_id end as last_non_direct_google_ads_extension_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_device else lnd_session.event_traffic_source_template_google_ads.google_ads_device end as last_non_direct_google_ads_device,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_device_model else lnd_session.event_traffic_source_template_google_ads.google_ads_device_model end as last_non_direct_google_ads_device_model,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_network else lnd_session.event_traffic_source_template_google_ads.google_ads_network end as last_non_direct_google_ads_network,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_location_physical else lnd_session.event_traffic_source_template_google_ads.google_ads_location_physical end as last_non_direct_google_ads_location_physical,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_location_interest else lnd_session.event_traffic_source_template_google_ads.google_ads_location_interest end as last_non_direct_google_ads_location_interest,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_placement else lnd_session.event_traffic_source_template_google_ads.google_ads_placement end as last_non_direct_google_ads_placement,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_target else lnd_session.event_traffic_source_template_google_ads.google_ads_target end as last_non_direct_google_ads_target,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_adtype else lnd_session.event_traffic_source_template_google_ads.google_ads_adtype end as last_non_direct_google_ads_adtype,
    case when lnd_session.id is null then s.event_traffic_source_template_google_ads.google_ads_video_source_id else lnd_session.event_traffic_source_template_google_ads.google_ads_video_source_id end as last_non_direct_google_ads_video_source_id,

    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_merchant_id else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_merchant_id end as last_non_direct_google_ads_merchant_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_product_channel else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_product_channel end as last_non_direct_google_ads_product_channel,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_product_id else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_product_id end as last_non_direct_google_ads_product_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_product_country else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_product_country end as last_non_direct_google_ads_product_country,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_product_language else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_product_language end as last_non_direct_google_ads_product_language,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_product_partition_id else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_product_partition_id end as last_non_direct_google_ads_product_partition_id,
    case when lnd_session.id is null then s.event_traffic_source_template_google_pmax_shopping.google_ads_store_code else lnd_session.event_traffic_source_template_google_pmax_shopping.google_ads_store_code end as last_non_direct_google_ads_store_code,

    -- TODO: some sort of switch for this?
    -- Google Ads Hotel (stuff)
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_hotelcenter_id else lnd_session.event_traffic_source_template_google_hotel.google_ads_hotelcenter_id end as last_non_direct_google_ads_hotelcenter_id,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_hotel_id else lnd_session.event_traffic_source_template_google_hotel.google_ads_hotel_id end as last_non_direct_google_ads_hotel_id,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_hotel_partition_id else lnd_session.event_traffic_source_template_google_hotel.google_ads_hotel_partition_id end as last_non_direct_google_ads_hotel_partition_id,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_hotel_adtype else lnd_session.event_traffic_source_template_google_hotel.google_ads_hotel_adtype end as last_non_direct_google_ads_hotel_adtype,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_start_day else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_start_day end as last_non_direct_google_ads_travel_start_day,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_start_month else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_start_month end as last_non_direct_google_ads_travel_start_month,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_start_year else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_start_year end as last_non_direct_google_ads_travel_start_year,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_end_day else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_end_day end as last_non_direct_google_ads_travel_end_day,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_end_month else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_end_month end as last_non_direct_google_ads_travel_end_month,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_travel_end_year else lnd_session.event_traffic_source_template_google_hotel.google_ads_travel_end_year end as last_non_direct_google_ads_travel_end_year,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_advanced_booking_window else lnd_session.event_traffic_source_template_google_hotel.google_ads_advanced_booking_window end as last_non_direct_google_ads_advanced_booking_window,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_date_type else lnd_session.event_traffic_source_template_google_hotel.google_ads_date_type end as last_non_direct_google_ads_date_type,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_number_of_adults else lnd_session.event_traffic_source_template_google_hotel.google_ads_number_of_adults end as last_non_direct_google_ads_number_of_adults,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_price_displayed_total else lnd_session.event_traffic_source_template_google_hotel.google_ads_price_displayed_total end as last_non_direct_google_ads_price_displayed_total,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_price_displayed_tax else lnd_session.event_traffic_source_template_google_hotel.google_ads_price_displayed_tax end as last_non_direct_google_ads_price_displayed_tax,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_user_currency else lnd_session.event_traffic_source_template_google_hotel.google_ads_user_currency end as last_non_direct_google_ads_user_currency,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_user_language else lnd_session.event_traffic_source_template_google_hotel.google_ads_user_language end as last_non_direct_google_ads_user_language,#}
{#    case when lnd_session.id is null then s.event_traffic_source_template_google_hotel.google_ads_rate_rule_id else lnd_session.event_traffic_source_template_google_hotel.google_ads_rate_rule_id end as last_non_direct_google_ads_rate_rule_id,#}

    -- Click ids
    case when lnd_session.id is null then s.event_traffic_source_click_ids.dclid else lnd_session.event_traffic_source_click_ids.dclid end as last_non_direct_dclid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.srsltid else lnd_session.event_traffic_source_click_ids.srsltid end as last_non_direct_srsltid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.msclkid else lnd_session.event_traffic_source_click_ids.msclkid end as last_non_direct_msclkid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.fbclid else lnd_session.event_traffic_source_click_ids.fbclid end as last_non_direct_fbclid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.gbraid else lnd_session.event_traffic_source_click_ids.gbraid end as last_non_direct_gbraid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.wbraid else lnd_session.event_traffic_source_click_ids.wbraid end as last_non_direct_wbraid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.li_fat_id else lnd_session.event_traffic_source_click_ids.li_fat_id end as last_non_direct_li_fat_id,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.twclid else lnd_session.event_traffic_source_click_ids.twclid end as last_non_direct_twclid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.rdt_cid else lnd_session.event_traffic_source_click_ids.rdt_cid end as last_non_direct_rdt_cid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.ttclid else lnd_session.event_traffic_source_click_ids.ttclid end as last_non_direct_ttclid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.ScCid else lnd_session.event_traffic_source_click_ids.ScCid end as last_non_direct_ScCid,
    case when lnd_session.id is null then s.event_traffic_source_click_ids.irclickid else lnd_session.event_traffic_source_click_ids.irclickid end as last_non_direct_irclickid,

    ------------------------------------------------------------
    -- END Last Non-Direct Attribution
    ------------------------------------------------------------

    e.last_event_at_utc as session_end_at_utc,
    greatest(ifnull(timestamp_diff(e.last_event_at_utc, s.session_start_at_utc, second),0), 0) as duration_seconds,
    e.is_engaged,
    1 - e.is_engaged as is_bounce,
    e.engaged_seconds,
    p.page_views,
    e.event_count,
from sessions s
left join engagement_metrics e on s.id = e.session_id
left join page_views p on s.id = p.session_id
left join find_session_attribution lst_atr on s.id = lst_atr.session_id
left join find_last_non_direct_session_id lnd on s.id = lnd.session_id
left join sessions lnd_session on lnd.last_non_direct_session_id = lnd_session.id
    {% if LAST_NON_DIRECT_LOOKBACK is not none %}
    and timestamp_diff(s.session_start_at_utc, lnd_session.session_start_at_utc, DAY) <= {{ LAST_NON_DIRECT_LOOKBACK }}
    {% endif %}