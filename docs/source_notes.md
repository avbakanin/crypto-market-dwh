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

## Symbol selection (whitelist) observations

Script: `ingestion/select_symbols.py` → `config/symbols.csv` (future source for `dim_symbol`).

7. **Incomplete history for new listings.** A symbol listed 3 months ago has ~2,200 hourly candles instead of ~8,760 per year; in cross-symbol comparisons this looks like gaps. → Rule: `first_candle_date` at least 365 days ago.
8. **Ticker ≠ asset.** Renames split history across symbols: LUNA → LUNC after the Terra collapse (May 2022), so `LUNCUSDT` starts on 2022-09-09. `first_candle_date` is the ticker's listing date, not the asset's. → Never merge symbols by base asset name without a mapping table.
9. **Non-crypto instruments on the spot market.** Stablecoins (new ones appear constantly, e.g. RWUSD in Oct 2026), gold-backed tokens (PAXG, XAUT), tokenized stocks/ETFs (NVDAB, QQQB, MSTRB, …). → Explicit exclusion list + data rule: 30d close range within [0.98, 1.02] ⇒ USD-pegged, excluded. Open issue: tokenized stocks are currently excluded only by the history rule.
10. **24h volume is a noisy liquidity metric.** MMTUSDT: 52.6M 24h vs 2.3M 30d average (×22); LUNCUSDT ×9; SKYUSDT ×8. A single snapshot gave 3 false positives out of 24. → Rule: 30-day average daily quote volume ≥ 10M USDT.
11. **Whitelist is a snapshot.** Volumes change daily, so re-running the script changes the list. → `config/symbols.csv` is committed to git and regenerated only deliberately; `selected_at_utc` records when.

## API behaviour observations

12. **Error semantics.** Errors return a JSON body `{"code": ..., "msg": ...}`, e.g. `HTTP 400 {"code":-1121,"msg":"Invalid symbol."}`. 4xx = our mistake, never retried. 429 = rate limit, may include `Retry-After` (seconds). 418 = IP ban after ignoring 429s, never retried (retrying extends the ban). 5xx = transient, retried with exponential backoff. → Implemented in `ingestion/clients/binance.py`.
13. **Expected row count is 8,759, not 8,760.** `startTime` is not aligned to the hour, and Binance returns candles with `open_time >= startTime`, so the first candle is the next full hour; the current hour is dropped as unclosed. Verified on all 23 symbols: 8,759 rows, 0 duplicates, 0 gaps (as of 2026-11-19 18:31 UTC). → Row-count checks must expect 8,759 for a 365-day window computed from a single as-of timestamp.
