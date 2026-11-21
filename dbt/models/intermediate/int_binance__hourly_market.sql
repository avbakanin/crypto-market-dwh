-- One row per (spot symbol, hour): returns plus the futures market at the END of the hour.
-- Every derived column is the state at hour_end = open_time + 1 h: open interest snapshot
-- at hour_end, the last funding settlement at or before hour_end. The state at the start
-- of an hour is therefore the previous row.

with hourly_returns as (

    select
        symbol,
        open_time,
        open_time_ms,
        open_time_ms + 3600000 as hour_end_ms,
        close_price,
        quote_volume,
        trade_count,
        return_1h,
        return_4h,
        return_zscore,
        return_baseline_hours,
        quote_volume_baseline,
        log_volume_zscore
    from {{ ref('int_binance__hourly_returns') }}

),

open_interest as (

    select
        spot_symbol,
        snapshot_time_ms,
        open_interest_coins,
        open_interest_usdt
    from {{ ref('stg_binance__open_interest_1h') }}

),

funding as (

    select
        spot_symbol,
        funding_hour,
        funding_hour_ms,
        funding_rate,
        interval_hours,
        interest_rate_8h
    from {{ ref('stg_binance__funding_rate') }}

),

hours_with_open_interest as (

    -- Joined before the ASOF join: the docs show no outer join chained after ASOF JOIN.
    -- Open interest exists only for the last ~30 days (source note 36): NULL before.
    select
        hourly_returns.*,
        open_interest.open_interest_coins,
        open_interest.open_interest_usdt
    from hourly_returns
    left join open_interest
        on
            hourly_returns.symbol = open_interest.spot_symbol
            and hourly_returns.hour_end_ms = open_interest.snapshot_time_ms

)

select
    hours.symbol,
    hours.open_time,
    hours.open_time_ms,
    hours.hour_end_ms,
    hours.close_price,
    hours.quote_volume,
    hours.trade_count,
    hours.return_1h,
    hours.return_4h,
    hours.return_zscore,
    hours.return_baseline_hours,
    hours.quote_volume_baseline,
    hours.log_volume_zscore,
    hours.open_interest_coins,
    hours.open_interest_usdt,
    funding.funding_hour as last_funding_hour,
    funding.funding_rate,
    funding.interval_hours as funding_interval_hours,
    -- Per 8 h, comparable across 4-hour and 8-hour contracts (source note 39).
    funding.funding_rate * 8 / funding.interval_hours as funding_rate_8h,
    -- Departure from the calm rate of this contract (source note 41: 0 for BNBUSDT).
    funding.funding_rate * 8 / funding.interval_hours - funding.interest_rate_8h
        as funding_premium_8h,
    ((hours.hour_end_ms - funding.funding_hour_ms) / 3600000)::number(10, 0)
        as hours_since_funding,
    -- The next settlement was due by hour_end and is not there: missed by the platform
    -- (2026-06-24) or not published yet at the tail. NULL before the funding history.
    (hours.hour_end_ms - funding.funding_hour_ms) / 3600000 >= funding.interval_hours
        as is_funding_stale
from hours_with_open_interest as hours
-- As-of join: the last settlement at or before the end of the hour. funding is unique per
-- (contract, hour) and contract <-> spot is one to one, so there are no ties to break.
asof join funding
    match_condition (hours.hour_end_ms >= funding.funding_hour_ms)
    on hours.symbol = funding.spot_symbol
