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
13. **Expected row count is 8,759, not 8,760.** `startTime` is not aligned to the hour, and Binance returns candles with `open_time >= startTime`, so the first candle is the next full hour; the current hour is dropped as unclosed. Verified on all 23 symbols: 8,759 rows, 0 duplicates, 0 gaps (as of 2026-11-19 18:31 UTC). Directly confirmed on 2026-11-20: for as-of 19:40:05 UTC (`startTime` = 2025-11-20 19:40:05) the first candle in RAW opens at 20:00:00, not at 19:00 (the candle containing `startTime`). → Row-count checks must expect 8,759 for a 365-day window computed from a single as-of timestamp.
14. **Without `startTime`, klines returns the most recent `limit` candles, and the last one is still open.** Checked on 2026-11-19 20:11 UTC: `BTCUSDT 1d limit=3` returned two closed days and today's candle with `close_time > now`. With `startTime=0, limit=1` the API returns the very first candle (BTCUSDT: 2017-08-17), i.e. the listing date. → `select_symbols.get_daily_stats` requests `limit = days + 1` and drops the last candle (`[:-1]`). Here `[:-1]` is correct, unlike paging by `startTime` (note #2), where full pages contain only closed candles. Caveat: if trading is halted, the last candle may already be closed, and `[:-1]` drops one good day.

## Platform observations: Snowflake

Trial account created 2026-11-20 (Standard edition, `AWS_US_EAST_2`). Setup script: `snowflake/setup.sql`.

15. **The role you pick is not the only role in effect.** Users get `DEFAULT_SECONDARY_ROLES = ('ALL')` by default, so every role granted to the user also authorizes statements. Under `CRYPTO_DWH_ROLE`, which has no `OWNERSHIP` on schemas, `DROP SCHEMA CRYPTO_DWH.RAW` succeeded; `ACCOUNT_USAGE.QUERY_HISTORY` shows `secondary_role_stats = {"roleCount":4,...}` for that statement. After `USE SECONDARY ROLES NONE` it fails with "Insufficient privileges ... must have OWNERSHIP". Snowsight also runs `USE SECONDARY ROLES ALL` in its own sessions; a worksheet keeps one session across runs, and `USE ROLE` does not reset secondary roles. → Least privilege must come from the roles granted to the user: the loader gets a dedicated service user with only the project role. Negative grant tests run with secondary roles off, otherwise they test the user, not the role.
16. **"Does not exist or not authorized" means either one.** `CREATE TABLE` in a dropped schema and `USE WAREHOUSE` without `USAGE` return the same kind of error: Snowflake does not reveal whether an object exists to a role that cannot see it. → When debugging, check existence as an admin role before suspecting grants (and the reverse).
17. **New trial accounts come with warehouses that idle for 5 minutes.** The signup form assigned the cloud region and edition without asking. Snowflake pre-provisioned `COMPUTE_WH` (the user's default) and `SNOWFLAKE_LEARNING_WH` with `AUTO_SUSPEND = 300`, plus `SYSTEM$STREAMLIT_NOTEBOOK_WH` with 60; all X-Small, Gen2 (`STANDARD_GEN_2`), owned by `ACCOUNTADMIN`. Gen2 is the default for new warehouses and costs 1.35x Gen1 credits on AWS; `STATEMENT_TIMEOUT_IN_SECONDS` defaults to 2 days. → `setup.sql` creates the project warehouse with explicit `GENERATION = '1'`, `AUTO_SUSPEND = 60`, a 600 s timeout, and applies the same idle and timeout limits to the pre-provisioned warehouses.
18. **Metadata statements do not wake the warehouse; `updated_on` is not a change log.** `USE`, `SHOW`, `CREATE`/`DROP TABLE` and context functions ran with `CRYPTO_WH` suspended (`resumed_on` stayed at the last real query). `updated_on` moved on a no-op `ALTER ... SET` with unchanged values and on every resume. → Count credits from warehouse metering, not from statement counts; track configuration drift by comparing properties with `setup.sql`, not by `updated_on`.
19. **A trial grants a role to PUBLIC, i.e. to every user.** Account provisioning grants `SNOWFLAKE_LEARNING_ROLE` to `PUBLIC`: `USAGE`/`OPERATE` on `SNOWFLAKE_LEARNING_WH` (Gen2, not under the project warehouse's daily monitor) and `CREATE SCHEMA` in `SNOWFLAKE_LEARNING_DB`. The service user `CRYPTO_LOADER`, granted only the project role, could still use it: `SHOW GRANTS TO USER` showed one role, while `CURRENT_AVAILABLE_ROLES()` in the smoke check returned three. → Check effective roles, not direct grants. `setup.sql` grants the role to the admin user and revokes it from `PUBLIC`; adding it to the expected roles would have legitimised the leak.
20. **A wrong key passphrase looks like a server-side rejection.** With the public key pasted into `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` by mistake, connector 5.2.0 raised `250001 (08001): Failed to login: Authentication failed. Unspecified`, yet `LOGIN_HISTORY_BY_USER` recorded no attempt (the next, successful login was recorded with `RSA_KEYPAIR`). The failure was apparently on the client, since the key could not be decrypted, but the message says otherwise. → On "Authentication failed", check the login history first: no record means a client-side problem (passphrase, key file); a record carries the server's `error_message`.
21. **COPY skips a file it loaded before only while the file is unchanged, and says so per file.** Load metadata is kept per table for 64 days (file name, size, ETag, rows). `FORCE` is described as loading files "regardless of whether they were loaded previously and haven't changed", so a changed file under the same name loads again. Repeating `COPY ... FILES = ('BTCUSDT.json.gz')` for a loaded file returned one row: `status = LOAD_SKIPPED`, `errors_seen = 1`, `first_error = "File was loaded before."`, and the table did not change. It did not return "Copy executed with 0 files processed." (2026-11-20). → Extract writes immutable batch directories (`<batch_id>/<SYMBOL>.json`), so a file never changes under a loaded name; the loader treats `LOAD_SKIPPED` as a stop that needs a human decision (`TRUNCATE` clears the load metadata, `FORCE` ignores it).
22. **PUT reports `SKIPPED` for a different file under an existing name, and keeps the old one.** With `OVERWRITE = FALSE`, connector 5.2.0: PUT of a new file → `UPLOADED`; the same file again → `SKIPPED`; a file with the same name and new content → also `SKIPPED`, with an empty `message`, and `LIST` still shows the original size and MD5 (experiment in a separate stage directory, 2026-11-20). The two cases cannot be told apart from the PUT result. → A batch directory is never rewritten (extract fails if it exists). Known limit: if a staged batch were edited locally (manifest included) and re-run, COPY would load the old staged version; only a changed row count would be caught by reconciliation. Comparing local and staged MD5 does not work: gzip output differs between compressors (header timestamp, settings).
23. **File metadata in COPY: relative path, MD5, element number.** `METADATA$FILENAME` is the path inside the stage (`20261120T194005Z/BTCUSDT.json.gz`), while `LIST` and the COPY result prefix the stage name (`klines_stage/20261120T194005Z/...`). `METADATA$FILE_CONTENT_KEY` equals the MD5 that `LIST` shows for the staged (compressed) file. With `STRIP_OUTER_ARRAY = TRUE`, `METADATA$FILE_ROW_NUMBER` is the 1-based position of the array element, in file order (1..8,759, `LAG` over it finds no out-of-order rows). → Parse paths from the end (`SPLIT_PART(..., -1)`), not by a fixed prefix; `_file_content_key` identifies the exact file version a row came from.
24. **`TIMESTAMP_LTZ` output follows the session time zone, which defaults to US Pacific.** `COPY_HISTORY.LAST_LOAD_TIME` and `created_on` in `SHOW` output came back as `-0800`, consistent with the default session time zone `America/Los_Angeles`. → Stored timestamps are converted explicitly: `_loaded_at = CONVERT_TIMEZONE('UTC', METADATA$START_SCAN_TIME)` (2-argument form returns `TIMESTAMP_TZ`), so RAW reads the same from any session (note #4).
25. **JSON strings stay strings in `VARIANT`.** Binance sends prices as JSON strings; in RAW `TYPEOF(payload[1])` is `VARCHAR` and `payload[1]::NUMBER(38, 8)` gives the exact decimal (`"86921.27000000"` → `86921.27000000`), while `open_time` is `INTEGER`. → Cast prices to `NUMBER(38, 8)` in staging, never through `FLOAT`/`TO_DOUBLE` (note #3).
