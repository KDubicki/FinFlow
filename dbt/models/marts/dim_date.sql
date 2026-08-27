with days as (
    select distinct date from {{ source('bronze', 'calendar_days') }}
)

select
    date as date_key,
    date,
    extract(year from date) as year,
    extract(month from date) as month,
    extract(day from date) as day,
    extract(dayofweek from date) as day_of_week,
    extract(quarter from date) as quarter,
    date = last_day(date) as is_month_end,
    exists (
        select 1 from {{ source('bronze', 'calendar_days') }} as c
        where c.calendar = 'XNYS' and c.date = days.date
    ) as trading_day_xnys
from days
