{{ config(
    materialized='incremental',
    unique_key=['symbol', 'date'],
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

{#- on_schema_change='fail' rather than append_new_columns: this table has an
    enforced contract, so a column appearing unannounced means the contract and
    the model disagree. Failing is the point. -#}

-- One row per instrument and date, carrying the latest opinion from the primary
-- source. Rows for sessions with no bar are excluded here and surfaced by the
-- gap test instead: a fact table of mostly-nulls is harder to reason about than
-- a missing row plus a check that says it is missing.

select
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    source,
    ingested_at,
    '{{ var("snapshot_id") }}' as snapshot_id
from {{ ref('int_calendar_aligned') }}
where not is_missing

{% if is_incremental() %}
  and date >= (select coalesce(max(date), date '1900-01-01') from {{ this }})
{% endif %}
