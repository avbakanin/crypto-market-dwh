# Source: Binance Spot API — klines

- Endpoint: `GET https://api.binance.com/api/v3/klines`
- Market data mirror: `https://data-api.binance.vision`
- Parameters: `symbol`, `interval`, `startTime`, `endTime`, `limit` (default 500, max 1000)
- Docs: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints

## Fields (array of 12 elements)

| # | Field | Response type | Example (BTCUSDT, 2026-11-19 15:00 UTC) |
|---|-------|---------------|------------------------------------------|
| 0 | open_time (ms, UTC) | int | 1795100400000 |
| 1 | open | string | "63558.00000000" |
| 2 | high | string | "63916.01000000" |
| 3 | low | string | "62780.07000000" |
| 4 | close | string | "63103.30000000" |
| 5 | volume (BTC) | string | "1213.38723000" |
| 6 | close_time (ms, UTC) | int | 1795103999999 |
| 7 | quote_volume (USDT) | string | "76724531.04007310" |
| 8 | trades_count | int | 206590 |
| 9 | taker_buy_base_volume | string | "631.83302000" |
| 10 | taker_buy_quote_volume | string | "39946372.67800280" |
| 11 | ignore | string | "0" |

## Data Quality observations

1. **The candle key is open_time.** Each candle covers `[open_time, open_time + 1h − 1ms]`; the next candle starts at `close_time + 1 ms`.
2. **The latest candle may still be open.** On 2026-11-19 at 17:22 UTC, the 17:00 candle showed a volume of 509 BTC vs ~1,200 for closed candles. Rule: load only candles with `close_time < now`.
3. **Prices and volumes are returned as strings** to preserve precision (quote_volume has up to 16 significant digits). Store as `NUMBER(38, 8)`; never cast to float.
4. **Timestamps are Unix epoch milliseconds in UTC.** Always pass `tz=timezone.utc` when converting in Python.
5. **Volume estimate:** one year of hourly candles ≈ 8,760 rows, i.e. at least 9 requests of 1,000 rows per symbol.
6. **A typo in a symbol may silently return another instrument.** `BTCUSD` (instead of `BTCUSDT`) returns valid data with a similar price (+0.2%) but ~16,000x lower volume. Mitigation: fixed symbol whitelist (`dim_symbol` + dbt `relationships` test) and a minimum liquidity threshold.