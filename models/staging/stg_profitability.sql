with
    source as (
        select * from {{ source("retails_raw_data_bq", "profitability_data_csv") }}
    )

select
    date_add(date '1899-12-30', interval cast(period as int64) day) as period_date,
    customer,
    `Item: Full Name` as product_name,
    `Item Description` as product_description,
    brand,
    model,
    color,
    specs,
    storage,
    `Sales Rep` as sales_rep,
    `Customer Country` as customer_country,
    safe_cast(quantity as numeric) as quantity,

    coalesce(
        safe_cast(`Total Revenue` as numeric),
        round(safe_cast(quantity as numeric) * safe_cast(`Unit Price` as numeric), 2)
    ) as revenue,

    coalesce(
        safe_cast(`Total Cost` as numeric),
        round(safe_cast(quantity as numeric) * safe_cast(`Unit Cost` as numeric), 2)
    ) as cost,

    (
        coalesce(safe_cast(`Total Revenue` as numeric), 0)
        - coalesce(safe_cast(`Total Cost` as numeric), 0)
    ) as profit

from source
where period is not null
