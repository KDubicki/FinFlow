{% test no_trading_day_gaps(model, symbol_column, date_column) %}
-- A bar missing on a day the exchange was open is an incident; a bar missing on
-- a holiday is expected. Only a calendar can tell the two apart, which is the
-- whole reason calendar_days exists.
--
-- Bounded to each instrument's own observed history so that a fund listing in
-- 2007 does not report every session since 1993 as a gap.

with bounds as (
    select
        {{ symbol_column }} as symbol,
        min({{ date_column }}) as first_seen,
        max({{ date_column }}) as last_seen
    from {{ model }}
    group by 1
),

expected as (
    select
        bounds.symbol,
        calendar_days.date
    from bounds
    inner join {{ source('bronze', 'registry_instruments') }} as instruments
        on bounds.symbol = instruments.symbol
    inner join {{ source('bronze', 'calendar_days') }} as calendar_days
        on instruments.calendar = calendar_days.calendar
    where calendar_days.date between bounds.first_seen and bounds.last_seen
)

select
    expected.symbol,
    expected.date
from expected
left join {{ model }} as actual
    on expected.symbol = actual.{{ symbol_column }}
   and expected.date = actual.{{ date_column }}
where actual.{{ date_column }} is null
{% endtest %}
