select
    source as source_key,
    source as name,
    case source
        when 'stooq' then 'primary'
        when 'fred' then 'primary'
        when 'twelvedata' then 'reconciliation'
        when 'synthetic' then 'synthetic'
        else 'unknown'
    end as tier
from (select distinct source from {{ ref('stg_ohlcv') }}
      union
      select distinct source from {{ ref('stg_macro') }})
