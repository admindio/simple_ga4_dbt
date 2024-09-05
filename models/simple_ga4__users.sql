{{
    config(
        materialized='view'
    )
}}
with first_attribution as (
    select
        user_pseudo_id,

        first_value(landing_page) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_landing_page,
        first_value(landing_page_query) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_landing_page_query,

        first_value(last_source) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_source,
        first_value(last_medium) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_medium,
        first_value(last_source) over (partition by user_pseudo_id order by session_start_at_utc asc) || ' / ' || first_value(last_medium) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_source_medium,
        first_value(last_campaign) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_campaign,
        first_value(last_content) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_content,
        first_value(last_term) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_term,
        first_value(last_campaign_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_campaign_id,
        first_value(last_referrer) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_referrer,
        first_value(last_gclid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_gclid,

        -- Flatten structs
        first_value(last_google_ads_customer_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_customer_id,
        first_value(last_google_ads_account_name) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_account_name,
        first_value(last_google_ads_ad_group_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_ad_group_id,
        first_value(last_google_ads_ad_group_name) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_ad_group_name,
        first_value(last_google_ads_matchtype) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_matchtype,
        first_value(last_google_ads_ad_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_ad_id,
        first_value(last_google_ads_extension_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_extension_id,
        first_value(last_google_ads_device) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_device,
        first_value(last_google_ads_device_model) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_device_model,
        first_value(last_google_ads_network) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_network,
        first_value(last_google_ads_location_physical) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_location_physical,
        first_value(last_google_ads_location_interest) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_location_interest,
        first_value(last_google_ads_placement) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_placement,
        first_value(last_google_ads_target) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_target,
        first_value(last_google_ads_adtype) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_adtype,
        first_value(last_google_ads_video_source_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_video_source_id,

        first_value(last_google_ads_merchant_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_merchant_id,
        first_value(last_google_ads_product_channel) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_product_channel,
        first_value(last_google_ads_product_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_product_id,
        first_value(last_google_ads_product_country) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_product_country,
        first_value(last_google_ads_product_language) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_product_language,
        first_value(last_google_ads_product_partition_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_product_partition_id,
        first_value(last_google_ads_store_code) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_store_code,
        -- TODO: some sort of switch for this?
        -- Google Ads Hotel (stuff)
{#        first_value(last_google_ads_hotelcenter_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_hotelcenter_id,#}
{#        first_value(last_google_ads_hotel_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_hotel_id,#}
{#        first_value(last_google_ads_hotel_partition_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_hotel_partition_id,#}
{#        first_value(last_google_ads_hotel_adtype) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_hotel_adtype,#}
{#        first_value(last_google_ads_travel_start_day) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_start_day,#}
{#        first_value(last_google_ads_travel_start_month) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_start_month,#}
{#        first_value(last_google_ads_travel_start_year) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_start_year,#}
{#        first_value(last_google_ads_travel_end_day) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_end_day,#}
{#        first_value(last_google_ads_travel_end_month) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_end_month,#}
{#        first_value(last_google_ads_travel_end_year) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_travel_end_year,#}
{#        first_value(last_google_ads_advanced_booking_window) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_advanced_booking_window,#}
{#        first_value(last_google_ads_date_type) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_date_type,#}
{#        first_value(last_google_ads_number_of_adults) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_number_of_adults,#}
{#        first_value(last_google_ads_price_displayed_total) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_price_displayed_total,#}
{#        first_value(last_google_ads_price_displayed_tax) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_price_displayed_tax,#}
{#        first_value(last_google_ads_user_currency) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_user_currency,#}
{#        first_value(last_google_ads_user_language) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_user_language,#}
{#        first_value(last_google_ads_rate_rule_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_google_ads_rate_rule_id,#}

        -- Click ids
        first_value(last_dclid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_dclid,
        first_value(last_srsltid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_srsltid,
        first_value(last_msclkid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_msclkid,
        first_value(last_fbclid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_fbclid,
        first_value(last_gbraid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_gbraid,
        first_value(last_wbraid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_wbraid,
        first_value(last_li_fat_id) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_li_fat_id,
        first_value(last_twclid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_twclid,
        first_value(last_rdt_cid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_rdt_cid,
        first_value(last_ttclid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_ttclid,
        first_value(last_ScCid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_ScCid,
        first_value(last_irclickid) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_irclickid,


        first_value(session_start_at_utc) over (partition by user_pseudo_id order by session_start_at_utc asc) as first_session_start_at_utc,
    from {{ ref("simple_ga4__sessions") }}
)
select
    s.user_pseudo_id as id,
    -- TODO canonical_user_id
    -- dimensions
    fst.* except(user_pseudo_id),

    -- metrics
    min(session_start_at_utc) as first_session_at_utc,
    max(session_start_at_utc) as last_session_at_utc,
    count(distinct id) as sessions,
    sum(page_views) as page_views,
    sum(duration_seconds) as total_session_duration,
    sum(engaged_seconds) as total_engaged_seconds,
    sum(event_count) as total_events,
from {{ ref("simple_ga4__sessions") }} s
    left join first_attribution fst on s.user_pseudo_id = fst.user_pseudo_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48
