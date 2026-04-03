{{ config(materialized='table') }}

with customers as (
    select distinct 
        customer as customer_name,
        customer_country as country
    from {{ ref('stg_profitability') }}
    union distinct
    select distinct 
        entity as customer_name,
        country
    from {{ ref('stg_group_sales') }}
)
select 
    md5(customer_name) as customer_key,
    customer_name,
    md5(country) as country_key
from customers
where customer_name is not null