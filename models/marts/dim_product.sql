{{ config(materialized='table') }}

select distinct
    md5(concat(product_name, brand, model)) as product_key,
    product_name,
    product_description,
    brand,
    model,
    color,
    specs,
    storage
from {{ ref('stg_profitability') }}