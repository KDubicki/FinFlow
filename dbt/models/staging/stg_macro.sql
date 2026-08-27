with source as (
    select * from {{ source('bronze', 'bronze_macro') }}
),

typed as (
    select
        cast(series_id as varchar)         as series_id,
        cast(observation_date as date)     as observation_date,
        cast(value as double)              as value,
        cast(vintage_date as date)         as vintage_date,
        cast(source as varchar)            as source,
        cast(ingested_at as timestamp)     as ingested_at,
        cast(ingestion_run_id as varchar)  as ingestion_run_id
    from source
),

ranked as (
    select
        typed.*,
        row_number() over (
            partition by
                series_id,
                observation_date,
                coalesce(vintage_date, date '1900-01-01'),
                source
            order by ingested_at desc
        ) as recency
    from typed
)

select
    series_id,
    observation_date,
    value,
    vintage_date,
    source,
    ingested_at,
    ingestion_run_id
from ranked
where recency = 1
