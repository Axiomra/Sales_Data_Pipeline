{{ config(materialized='table') }}

with countries as (
    select distinct customer_country as country_name from {{ ref('stg_profitability') }}
    union distinct
    select distinct country as country_name from {{ ref('stg_group_sales') }}
)
select 
    md5(country_name) as country_key,
    country_name
from countries
where country_name is not null