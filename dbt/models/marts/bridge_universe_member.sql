-- Date-effective membership. A backtest of `sectors` starting in 2010 must hold
-- nine members, not eleven (PROJECT.md §5.3), and that is only possible if the
-- validity window travels with the row rather than being applied at read time.

select
    universe as universe_key,
    symbol as instrument_key,
    coalesce(valid_from, date '1900-01-01') as valid_from,
    coalesce(valid_to, date '9999-12-31') as valid_to
from {{ source('bronze', 'registry_universe_members') }}
