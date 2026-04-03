{{ config(materialized='table') }}

select
    md5(concat(cast(transaction_id as string), cast(date as string))) as sales_key,
    transaction_id,
    cast(date as date) as date_key,
    md5(entity) as customer_key,
    md5(country) as country_key,
    md5(subsidiary) as subsidiary_key,
    md5(class) as class_key,
    quantity,
    unit_price,
    amount
from {{ ref('stg_group_sales') }}