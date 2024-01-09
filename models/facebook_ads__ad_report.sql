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

ad_sets as (

    select *
    from {{ var('ad_set_history') }}
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
        ad_sets.ad_set_id,
        ad_sets.ad_set_name,
        ads.ad_id,
        ads.ad_name,
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
    left join ad_sets
        on ads.ad_set_id = ad_sets.ad_set_id
        and ads.source_relation = ad_sets.source_relation
    {{ dbt_utils.group_by(10) }}
)

select 
		ads.source_relation,
		ads.date_day,
		ads.account_id,
		ads.account_name,
		ads.campaign_id,
		ads.campaign_name,
		ads.ad_set_id,
		ads.ad_set_name,
		ads.ad_id,
		ads.ad_name,
        sum(ads.clicks) as clicks,
        sum(ads.impressions) as impressions,
        sum(ads.spend) as spend,
        SUM(conversion.value) as conversions

         FROM joined ads
         LEFT JOIN  {{ ref('stg_facebook_ads__conversion_data_conversions') }} conversion
        ON ads.ad_id= CAST(conversion.ad_id As INT) and ads.date_day=conversion.date
GROUP BY 1,2,3,4,5,6,7,8,9,10
