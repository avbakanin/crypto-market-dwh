-- One row per whitelisted spot symbol, with its USD-M perpetual where one exists.
-- Both seeds are snapshots (selected_at_utc, mapped_at_utc): Binance serves no history of
-- the multiplier or the interest rate, and the selection volumes are inputs of that day.
-- Kept as type 1 for now; SCD2 is an open question (CLAUDE.md, section 11).

select
    symbols.symbol,
    symbols.base_asset,
    symbols.quote_asset,
    symbols.first_candle_date,
    perpetuals.futures_symbol,
    perpetuals.contract_multiplier,
    perpetuals.interest_rate_8h,
    perpetuals.futures_symbol is not null as has_perpetual,
    symbols.avg_quote_volume_30d_usdt,
    symbols.selected_at_utc,
    perpetuals.mapped_at_utc
from {{ ref('symbols') }} as symbols
-- left join: a spot symbol without a perpetual stays, it just has no funding or OI.
left join {{ ref('perpetuals') }} as perpetuals
    on symbols.symbol = perpetuals.spot_symbol
