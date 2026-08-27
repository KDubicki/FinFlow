-- Every session each instrument's exchange was open, with the bar if we have it.
--
-- A left join from the calendar rather than from the bars: that is what turns a
-- missing bar into a visible null instead of an absent row nobody counts.

with sessions as (
    select
        instruments.symbol,
        calendar_days.date
    from {{ source('bronze', 'registry_instruments') }} as instruments
    inner join {{ source('bronze', 'calendar_days') }} as calendar_days
        on instruments.calendar = calendar_days.calendar
    where calendar_days.date >= instruments.backfill_start
      and (instruments.delisted is null or calendar_days.date <= cast(instruments.delisted as date))
),

bars as (
    select * from {{ ref('int_source_reconciled') }}
)

select
    sessions.symbol,
    sessions.date,
    bars.source,
    bars.open,
    bars.high,
    bars.low,
    bars.close,
    bars.volume,
    bars.ingested_at,
    bars.ingestion_run_id,
    bars.close is null as is_missing
from sessions
left join bars
    on sessions.symbol = bars.symbol
   and sessions.date = bars.date
