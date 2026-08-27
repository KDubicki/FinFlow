-- Macro observations dated by when they were *available*, not when they refer to.
--
-- CPIAUCSL for March is dated 1 March and published around 10 April. Joining on
-- the observation date reads a number that did not exist for six weeks, which
-- is the most easily-violated form of lookahead in the whole system and the one
-- that looks most correct while being wrong (PROJECT.md §6.3).

with observations as (
    select * from {{ ref('stg_macro') }}
),

series as (
    select series_id, release_lag_days, revised, vintage_aware
    from {{ source('bronze', 'registry_macro_series') }}
)

select
    observations.series_id,
    observations.observation_date,
    cast(
        observations.observation_date + cast(series.release_lag_days as integer)
        as date
    ) as available_from,
    observations.value,
    observations.vintage_date,
    series.revised as is_revised,
    observations.source
from observations
inner join series using (series_id)
