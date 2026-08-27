-- Typing and renaming only. Any DuckDB-specific SQL in this project belongs
-- here or in a macro, never in marts -- that is what keeps the claim "the marts
-- survive a lakehouse migration" falsifiable rather than decorative.

with source as (
    select * from {{ source('bronze', 'bronze_ohlcv') }}
),

typed as (
    select
        cast(symbol as varchar)            as symbol,
        cast(date as date)                 as date,
        cast(source as varchar)            as source,
        cast(open as double)               as open,
        cast(high as double)               as high,
        cast(low as double)                as low,
        cast(close as double)              as close,
        cast(volume as double)             as volume,
        cast(ingested_at as timestamp)     as ingested_at,
        cast(ingestion_run_id as varchar)  as ingestion_run_id
    from source
),

deduplicated as (
    -- The loader already resolves to the latest reading per key; this is a
    -- belt-and-braces guard so a hand-loaded table cannot break the grain.
    select *
    from typed
    qualify row_number() over (
        partition by symbol, date, source
        order by ingested_at desc
    ) = 1
)

select * from deduplicated
