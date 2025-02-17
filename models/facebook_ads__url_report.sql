{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True),
    unique_key = ['source_relation','date_day','account_id','campaign_id','ad_set_id','ad_id'],
    partition_by={
      "field": "date_day", 
      "data_type": "date",
      "granularity": "day"
    }
    ) }}

with report as (

    select *
    from {{ var('basic_ad') }}

), 

creatives as (

    select *
    from {{ ref('int_facebook_ads__creative_history') }}

), 

accounts as (

    select *
    from {{ var('account_history') }}
    where is_most_recent_record = true

), 

ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = true

), 

ad_sets as (

    select *
    from {{ var('ad_set_history') }}
    where is_most_recent_record = true

), 

campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true

), 

joined as (

    select
        report.source_relation,
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        campaigns.campaign_id,
        campaigns.campaign_name,
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
        creatives.creative_id,
        creatives.creative_name,
        creatives.base_url,
        creatives.url_host,
        creatives.url_path,
        creatives.utm_source,
        creatives.utm_medium,
        creatives.utm_campaign,
        creatives.utm_content,
        creatives.utm_term,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='facebook_ads__basic_ad_passthrough_metrics', transform = 'sum') }}
    from report
    left join ads 
        on report.ad_id = ads.ad_id
        and report.source_relation = ads.source_relation
    left join creatives
        on ads.creative_id = creatives.creative_id
        and ads.source_relation = creatives.source_relation
    left join ad_sets
        on ads.ad_set_id = ad_sets.ad_set_id
        and ads.source_relation = ad_sets.source_relation
    left join campaigns
        on ads.campaign_id = campaigns.campaign_id
        and ads.source_relation = campaigns.source_relation
    left join accounts
        on report.account_id = accounts.account_id
        and report.source_relation = accounts.source_relation  

    {% if var('ad_reporting__url_report__using_null_filter', True) %}
        where creatives.url is not null
    {% endif %}
    
    {{ dbt_utils.group_by(20) }}
)

-- addition for conversion data
select 
    url.source_relation,
    url.date_day,
    url.account_id,
    url.account_name,
    url.campaign_id,
    url.campaign_name,
    url.ad_set_id,
    url.ad_set_name,
    url.ad_id,
    url.ad_name,
    url.creative_id,
    url.creative_name,
    url.base_url,
    url.url_host,
    url.url_path,
    url.utm_source,
    url.utm_medium,
    url.utm_campaign,
    url.utm_content,
    url.utm_term,
    url.clicks,
    url.impressions,
    url.spend,
    SUM(conversion.value) as conversions

    FROM joined url
    LEFT JOIN  {{ ref('stg_facebook_ads__conversion_data_conversions') }} conversion
    ON url.ad_id= CAST(conversion.ad_id As INT) and url.date_day=conversion.date
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23


