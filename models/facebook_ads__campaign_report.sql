{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True),
    unique_key = ['source_relation','date_day','account_id','campaign_id'],
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

accounts as (

    select *
    from {{ var('account_history') }}
    where is_most_recent_record = true

),

campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = true

),

ads as (

    select *
    from {{ var('ad_history') }}
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
        campaigns.start_at,
        campaigns.end_at,
        campaigns.status,
        campaigns.daily_budget,
        campaigns.lifetime_budget,
        campaigns.budget_remaining,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='facebook_ads__basic_ad_passthrough_metrics', transform = 'sum') }}
    from report 
    left join accounts
        on report.account_id = accounts.account_id
        and report.source_relation = accounts.source_relation
    left join ads 
        on report.ad_id = ads.ad_id
        and report.source_relation = ads.source_relation
    left join campaigns
        on ads.campaign_id = campaigns.campaign_id
        and ads.source_relation = campaigns.source_relation
    {{ dbt_utils.group_by(12) }}
)

-- addition for conversion data
select 
        campaign.source_relation,
        campaign.date_day,
        campaign.account_id,
        campaign.account_name,
        campaign.campaign_id,
        campaign.campaign_name,
        campaign.start_at,
        campaign.end_at,
        campaign.status,
        campaign.daily_budget,
        campaign.lifetime_budget,
        campaign.budget_remaining,
        campaign.clicks,
        campaign.impressions,
        campaign.spend,
        SUM(conversion.value) as conversions

         FROM joined  campaign
        LEFT JOIN {{ ref('stg_facebook_ads__conversion_data') }} conv_data
         ON campaign.account_id = conv_data.account_id  and campaign.campaign_id=conv_data.campaign_id 
         and campaign.date_day= conv_data.date 
        LEFT JOIN  {{ ref('stg_facebook_ads__conversion_data_conversions') }} conversion
        ON conv_data.ad_id= conversion.ad_id  and conv_data.date=conversion.date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
