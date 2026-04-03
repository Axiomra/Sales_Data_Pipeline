{{ config(materialized='table') }}

with date_raw as (
    select distinct cast(date as date) as date_val 
    from {{ ref('stg_group_sales') }}
    
    union distinct
    
    select distinct cast(period_date as date) as date_val 
    from {{ ref('stg_profitability') }}
)

select 
    date_val as date_key,
    -- US Client Format: 16-March-2025
    format_date('%d-%B-%Y', date_val) as full_date, 
    extract(year from date_val) as year,
    extract(month from date_val) as month,
    format_date('%B', date_val) as month_name,
    extract(quarter from date_val) as quarter,
    extract(day from date_val) as day,
    -- Week number (00-53)
    format_date('%U', date_val) as week
from date_raw
where date_val is not null
order by date_val desc