{{ config(materialized='table') }}

select distinct
    md5(sales_rep) as sales_rep_key,
    sales_rep as sales_rep_name
from {{ ref('stg_profitability') }}
where sales_rep is not null