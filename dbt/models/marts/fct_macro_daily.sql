-- First release per observation: the number as it was first published, which is
-- what a decision made that day could have used.
--
-- Written as a row_number CTE rather than QUALIFY. QUALIFY is DuckDB and
-- Snowflake only, and mart SQL stays dialect-neutral so the claim that these
-- models survive a lakehouse migration is falsifiable (PROJECT.md §10).

with released as (
    select * from {{ ref('int_macro_released') }}
),

ranked as (
    select
        released.*,
        row_number() over (
            partition by series_id, observation_date
            order by coalesce(vintage_date, date '1900-01-01')
        ) as release_rank
    from released
)

select
    series_id,
    observation_date,
    available_from,
    value,
    vintage_date,
    is_revised,
    '{{ var("snapshot_id") }}' as snapshot_id
from ranked
where release_rank = 1
