{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True),
    unique_key = ['source_relation','date_day','account_id'],
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

joined as (

    select 
        report.source_relation,
        report.date_day,
        accounts.account_id,
        accounts.account_name,
        accounts.account_status,
        accounts.business_country_code,
        accounts.created_at,
        accounts.currency,
        accounts.timezone_name,
        sum(report.clicks) as clicks,
        sum(report.impressions) as impressions,
        sum(report.spend) as spend

        {{ fivetran_utils.persist_pass_through_columns(pass_through_variable='facebook_ads__basic_ad_passthrough_metrics', transform = 'sum') }}
    from report 
    left join accounts
        on report.account_id = accounts.account_id
        and report.source_relation = accounts.source_relation
    {{ dbt_utils.group_by(9) }}
)

-- addition for conversion data
select 
       joined.source_relation,
       joined.date_day,
       joined.account_id,
       joined.account_name,
       joined.account_status,
       joined.business_country_code,
       joined.created_at,
       joined.currency,
       joined.timezone_name,
       joined.clicks,
       joined.impressions,
       joined.spend,
       sum(conversion.value) as conversions

         FROM joined 
         LEFT JOIN {{ ref('stg_facebook_ads__conversion_data') }} conv_data
         ON joined.account_id = conv_data.account_id and joined.date_day= conv_data.date
        LEFT JOIN  {{ ref('stg_facebook_ads__conversion_data_conversions') }} conversion
        ON conv_data.ad_id= conversion.ad_id  and conv_data.date=conversion.date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
