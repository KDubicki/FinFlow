-- Do two vendors agree about the same bar?
--
-- The primary source always wins on write; a secondary only ever produces a
-- flag (PROJECT.md §6.6). This model is that flag, and its value is not the
-- rows it returns on a good day -- it is the row it returns on the day one
-- vendor silently changes an adjustment convention, which is otherwise
-- invisible until a backtest disagrees with reality months later.
--
-- Divergence is measured in basis points and compared against a per-asset-class
-- threshold, because 25 bps between two quotes on SPY is a defect and 25 bps on
-- UNG is a Tuesday. A single global number would either drown the equity
-- signals or never fire on the commodities.
--
-- Scoped to a rolling window: reconciliation is a *current* data-quality
-- question. A vendor disagreeing about a bar from 2011 is a fact about history
-- nobody is going to fix, and carrying it forever would turn a check into a
-- backlog.

{{ config(materialized='table') }}

with bars as (

    select
        symbol,
        date,
        source,
        close
    from {{ ref('stg_ohlcv') }}

),

instruments as (

    select
        symbol,
        primary_source,
        asset_class
    from {{ source('bronze', 'registry_instruments') }}

),

window_bounds as (

    -- 45 calendar days is the smallest window that reliably contains 30
    -- trading sessions across every calendar in the registry, holidays
    -- included. Deliberately generous: the cost of a wider window is a few
    -- extra rows, and the cost of a narrower one is a check that quietly
    -- inspects three weeks.
    select cast(max(date) as date) - 45 as earliest
    from bars

),

readings as (

    select
        bars.symbol,
        bars.date,
        bars.source,
        bars.close,
        instruments.primary_source,
        instruments.asset_class
    from bars
    inner join instruments on bars.symbol = instruments.symbol
    cross join window_bounds
    where bars.date >= window_bounds.earliest

),

primary_reading as (

    select
        symbol,
        date,
        source as primary_source,
        close as primary_close
    from readings
    where source = primary_source

),

secondary_reading as (

    select
        symbol,
        date,
        asset_class,
        source as secondary_source,
        close as secondary_close
    from readings
    where source <> primary_source

),

compared as (

    select
        secondary_reading.symbol,
        secondary_reading.date,
        secondary_reading.asset_class,
        primary_reading.primary_source,
        secondary_reading.secondary_source,
        primary_reading.primary_close,
        secondary_reading.secondary_close,
        abs(secondary_reading.secondary_close - primary_reading.primary_close)
            / nullif(primary_reading.primary_close, 0) * 10000 as divergence_bps,
        -- Cast because the thresholds arrive from dbt vars as integers, and
        -- the enforced contract declares a double. A silent widening would be
        -- fine here and an unenforced contract would not be.
        cast(case secondary_reading.asset_class
            when 'equity' then {{ var('divergence_bps_equity') }}
            when 'rates' then {{ var('divergence_bps_rates') }}
            when 'credit' then {{ var('divergence_bps_credit') }}
            when 'commodity' then {{ var('divergence_bps_commodity') }}
            when 'currency' then {{ var('divergence_bps_currency') }}
            else {{ var('divergence_bps_equity') }}
        end as double) as threshold_bps
    from secondary_reading
    inner join primary_reading
        on secondary_reading.symbol = primary_reading.symbol
        and secondary_reading.date = primary_reading.date

)

select
    symbol,
    date,
    asset_class,
    primary_source,
    secondary_source,
    primary_close,
    secondary_close,
    divergence_bps,
    threshold_bps,
    -- Reported, never blocking. A divergence means "look at this", and a
    -- reconciliation source that could stop the pipeline would make the
    -- system less reliable by adding a vendor to it.
    case when divergence_bps > threshold_bps then true else false end as breaches_threshold,
    '{{ var("snapshot_id") }}' as snapshot_id
from compared
