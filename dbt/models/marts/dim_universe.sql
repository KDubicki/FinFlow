select distinct
    universe as universe_key,
    universe as name,
    description,
    benchmark_symbol
from {{ source('bronze', 'registry_universe_members') }}
