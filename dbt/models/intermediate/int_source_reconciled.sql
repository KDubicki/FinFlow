-- Choose the reading that counts.
--
-- An instrument may declare several vendors. The primary always wins on write;
-- a secondary only ever produces a reconciliation flag (PROJECT.md §6.6). That
-- decision lives here rather than in the loader, so bronze keeps every vendor's
-- reading and the choice stays re-runnable.

with bars as (
    select * from {{ ref('stg_ohlcv') }}
),

instruments as (
    select symbol, primary_source
    from {{ source('bronze', 'registry_instruments') }}
),

ranked as (
    select
        bars.*,
        instruments.primary_source,
        case when bars.source = instruments.primary_source then 0 else 1 end as source_rank
    from bars
    inner join instruments using (symbol)
)

select
    symbol,
    date,
    source,
    open,
    high,
    low,
    close,
    volume,
    ingested_at,
    ingestion_run_id
from ranked
qualify row_number() over (partition by symbol, date order by source_rank, source) = 1
