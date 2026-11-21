-- One row per (pump event, hour offset) from -pump_window_hours to +pump_window_hours:
-- the hourly profile around each event, the shape the dashboard averages by offset.
-- Offset 0 is the event hour. Values relative to the event start (end of hour -1).
-- Every event gets every offset; hours outside the loaded history are NULL, not missing,
-- so "rows = events x offsets" holds and averages see the gaps.

{%- set window = var('pump_window_hours') %}

with events as (

    select
        pump_event_id,
        symbol,
        event_open_time_ms,
        close_price_before,
        quote_volume_baseline,
        open_interest_coins_before
    from {{ ref('fct_pump_events') }}

),

offsets as (

    -- row_number, not seq4(): seq4() may have gaps.
    select row_number() over (order by seq4()) - 1 - {{ window }} as offset_hours
    from table(generator(rowcount => {{ 2 * window + 1 }}))

),

market as (

    select
        symbol,
        open_time,
        open_time_ms,
        close_price,
        quote_volume,
        return_1h,
        open_interest_coins,
        funding_rate_8h,
        funding_premium_8h,
        is_funding_stale
    from {{ ref('int_binance__hourly_market') }}

)

select
    events.pump_event_id,
    events.symbol,
    offsets.offset_hours,
    events.event_open_time_ms + offsets.offset_hours * 3600000 as open_time_ms,
    market.open_time,
    market.return_1h,
    market.close_price / events.close_price_before - 1 as return_since_start,
    market.quote_volume / events.quote_volume_baseline as volume_ratio,
    market.open_interest_coins / nullif(events.open_interest_coins_before, 0) - 1
        as open_interest_change_since_start,
    market.funding_rate_8h,
    market.funding_premium_8h,
    market.is_funding_stale
from events
cross join offsets
left join market
    on
        events.symbol = market.symbol
        and events.event_open_time_ms + offsets.offset_hours * 3600000 = market.open_time_ms
