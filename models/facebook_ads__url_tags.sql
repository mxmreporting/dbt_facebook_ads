{{ config(enabled=var('ad_reporting__facebook_ads_enabled', True),
    unique_key = ['source_relation','_fivetran_id','key','type'],
    partition_by={
      "field": "creative_id",
      "data_type": "integer"
    }
    ) }}

with base as (

    select *
    from {{ var('creative_history') }}
    where is_most_recent_record = true
), 

required_fields as (

    select
        source_relation,
        _fivetran_id,
        creative_id,
        url_tags
    from base
    where url_tags is not null
), 

{{ get_url_tags_query() }} 

select *
from fields
