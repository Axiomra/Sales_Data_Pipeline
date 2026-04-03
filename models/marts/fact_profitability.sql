{{ config(materialized='table') }}

select
    md5(concat(cast(period_date as string), customer, product_name)) as profitability_key,
    period_date as date_key,
    md5(customer) as customer_key,
    md5(concat(product_name, brand, model)) as product_key,
    md5(sales_rep) as sales_rep_key,
    md5(customer_country) as country_key,
    quantity,
    revenue,
    cost,
    profit
from {{ ref('stg_profitability') }}