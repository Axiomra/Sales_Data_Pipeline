with source as (
    select * from {{ source('retails_raw_data_bq', 'group_sales_report_csv') }}
)

select
    safe_cast(Trandate as date) as date,
    Tranid as transaction_id,
    Entity,
    cast(round(safe_cast(Unit_Price as numeric)) as int64) as unit_price,
    cast(round(safe_cast(Quantity as numeric)) as int64) as quantity,
    round(safe_cast(Amount as numeric), 2) as amount,
    Class,
    ifnull(Country, 'Unknown') as country,
    Subsidiary as subsidiary
from source
where Tranid is not null