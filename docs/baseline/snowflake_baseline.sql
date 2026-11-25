-- Snowflake baseline of crypto-market-dwh before the trial ends.
-- One row per metric: layer, relation, grain, metric, compare, value.
-- Read-only; runs on COMPUTE_WH so the CRYPTO_WH metering stays clean.
use role accountadmin;
use secondary roles none;
use warehouse compute_wh;

with last_invocations as (
    select dbt_command, max_by(invocation_id, run_started_at) as invocation_id
    from CRYPTO_DWH.DQ.DQ_CHECK_RESULTS
    where dbt_command in ('build', 'test')
    group by dbt_command
),

last_invocation_results as (
    select results.*
    from CRYPTO_DWH.DQ.DQ_CHECK_RESULTS as results
    inner join last_invocations using (dbt_command, invocation_id)
),

metrics as (
select 0 as ord, 'meta' as layer, 'session' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('captured_at_utc', 'snowflake_only', coalesce(to_varchar(to_varchar(convert_timezone('UTC', current_timestamp()), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')), 'null')),
            array_construct('snowflake_version', 'snowflake_only', coalesce(to_varchar(current_version()), 'null')),
            array_construct('role', 'snowflake_only', coalesce(to_varchar(current_role()), 'null')),
            array_construct('warehouse', 'snowflake_only', coalesce(to_varchar(current_warehouse()), 'null'))
        ) as a
) as s, lateral flatten(input => s.a) as m

union all

select 1 as ord, 'raw' as layer, 'binance_spot_klines_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, payload[0]::number(38, 0))), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(payload[0]::number(38, 0))), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(payload[0]::number(38, 0))), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(payload[0]::number(38, 0))), 'null')),
            array_construct('sum_close', 'exact', coalesce(to_varchar(sum(payload[4]::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_SPOT_KLINES_1H
) as s, lateral flatten(input => s.a) as m

union all

select 2 as ord, 'raw' as layer, 'binance_futures_funding_rate' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, floor(payload:fundingTime::number(38, 0) / 3600000) * 3600000)), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(floor(payload:fundingTime::number(38, 0) / 3600000) * 3600000)), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(floor(payload:fundingTime::number(38, 0) / 3600000) * 3600000)), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(floor(payload:fundingTime::number(38, 0) / 3600000) * 3600000)), 'null')),
            array_construct('sum_funding_rate', 'exact', coalesce(to_varchar(sum(payload:fundingRate::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_FUTURES_FUNDING_RATE
) as s, lateral flatten(input => s.a) as m

union all

select 3 as ord, 'raw' as layer, 'binance_futures_open_interest_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, payload:timestamp::number(38, 0))), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(payload:timestamp::number(38, 0))), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_open_interest', 'exact', coalesce(to_varchar(sum(payload:sumOpenInterest::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_FUTURES_OPEN_INTEREST_1H
) as s, lateral flatten(input => s.a) as m

union all

select 4 as ord, 'raw' as layer, 'binance_futures_top_account_ratio_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, payload:timestamp::number(38, 0))), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(payload:timestamp::number(38, 0))), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_long_short_ratio', 'exact', coalesce(to_varchar(sum(payload:longShortRatio::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_FUTURES_TOP_ACCOUNT_RATIO_1H
) as s, lateral flatten(input => s.a) as m

union all

select 5 as ord, 'raw' as layer, 'binance_futures_top_position_ratio_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, payload:timestamp::number(38, 0))), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(payload:timestamp::number(38, 0))), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_long_short_ratio', 'exact', coalesce(to_varchar(sum(payload:longShortRatio::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_FUTURES_TOP_POSITION_RATIO_1H
) as s, lateral flatten(input => s.a) as m

union all

select 6 as ord, 'raw' as layer, 'binance_futures_global_account_ratio_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('batches', 'exact', coalesce(to_varchar(count(distinct _batch_id)), 'null')),
            array_construct('first_batch_id', 'exact', coalesce(to_varchar(min(_batch_id)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, payload:timestamp::number(38, 0))), 'null')),
            array_construct('min_key_ms', 'exact', coalesce(to_varchar(min(payload:timestamp::number(38, 0))), 'null')),
            array_construct('max_key_ms', 'exact', coalesce(to_varchar(max(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_key_ms', 'exact', coalesce(to_varchar(sum(payload:timestamp::number(38, 0))), 'null')),
            array_construct('sum_long_short_ratio', 'exact', coalesce(to_varchar(sum(payload:longShortRatio::number(38, 8))), 'null')),
            array_construct('content_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(symbol, payload, _batch_id, _file_row_number)), 'null'))
        ) as a
        from CRYPTO_DWH.RAW.BINANCE_FUTURES_GLOBAL_ACCOUNT_RATIO_1H
) as s, lateral flatten(input => s.a) as m

union all

select 7 as ord, 'staging' as layer, 'stg_binance__klines_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, open_time_ms)), 'null')),
            array_construct('min_open_time_ms', 'exact', coalesce(to_varchar(min(open_time_ms)), 'null')),
            array_construct('max_open_time_ms', 'exact', coalesce(to_varchar(max(open_time_ms)), 'null')),
            array_construct('sum_open_time_ms', 'exact', coalesce(to_varchar(sum(open_time_ms)), 'null')),
            array_construct('sum_close_price', 'exact', coalesce(to_varchar(sum(close_price)), 'null')),
            array_construct('sum_base_volume', 'exact', coalesce(to_varchar(sum(base_volume)), 'null')),
            array_construct('sum_quote_volume', 'exact', coalesce(to_varchar(sum(quote_volume)), 'null')),
            array_construct('sum_version_count', 'exact', coalesce(to_varchar(sum(_version_count)), 'null')),
            array_construct('revised_keys', 'exact', coalesce(to_varchar(count_if(_is_revised)), 'null')),
            array_construct('last_batch_id', 'exact', coalesce(to_varchar(max(_batch_id)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__KLINES_1H
) as s, lateral flatten(input => s.a) as m

union all

select 8 as ord, 'staging' as layer, 'stg_binance__klines_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select symbol::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('min_open_time_ms', 'exact', coalesce(to_varchar(min(open_time_ms)), 'null')),
            array_construct('max_open_time_ms', 'exact', coalesce(to_varchar(max(open_time_ms)), 'null')),
            array_construct('sum_close_price', 'exact', coalesce(to_varchar(sum(close_price)), 'null')),
            array_construct('sum_quote_volume', 'exact', coalesce(to_varchar(sum(quote_volume)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__KLINES_1H
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 9 as ord, 'staging' as layer, 'stg_binance__funding_rate' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('contracts', 'exact', coalesce(to_varchar(count(distinct contract_symbol)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct contract_symbol, funding_hour_ms)), 'null')),
            array_construct('min_funding_hour_ms', 'exact', coalesce(to_varchar(min(funding_hour_ms)), 'null')),
            array_construct('max_funding_hour_ms', 'exact', coalesce(to_varchar(max(funding_hour_ms)), 'null')),
            array_construct('sum_funding_rate', 'exact', coalesce(to_varchar(sum(funding_rate)), 'null')),
            array_construct('count_mark_price', 'exact', coalesce(to_varchar(count(mark_price)), 'null')),
            array_construct('sum_mark_price', 'exact', coalesce(to_varchar(sum(mark_price)), 'null')),
            array_construct('sum_mark_price_per_coin', 'tolerance', coalesce(to_varchar(round(sum(mark_price_per_coin), 6)::number(38, 6)), 'null')),
            array_construct('sum_interval_hours', 'exact', coalesce(to_varchar(sum(interval_hours)), 'null')),
            array_construct('after_missed_settlement', 'exact', coalesce(to_varchar(count_if(is_after_missed_settlement)), 'null')),
            array_construct('revised_keys', 'exact', coalesce(to_varchar(count_if(_is_revised)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__FUNDING_RATE
) as s, lateral flatten(input => s.a) as m

union all

select 10 as ord, 'staging' as layer, 'stg_binance__open_interest_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('contracts', 'exact', coalesce(to_varchar(count(distinct contract_symbol)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct contract_symbol, snapshot_time_ms)), 'null')),
            array_construct('min_snapshot_time_ms', 'exact', coalesce(to_varchar(min(snapshot_time_ms)), 'null')),
            array_construct('max_snapshot_time_ms', 'exact', coalesce(to_varchar(max(snapshot_time_ms)), 'null')),
            array_construct('sum_open_interest', 'exact', coalesce(to_varchar(sum(open_interest)), 'null')),
            array_construct('sum_open_interest_coins', 'exact', coalesce(to_varchar(sum(open_interest_coins)), 'null')),
            array_construct('sum_open_interest_usdt', 'exact', coalesce(to_varchar(sum(open_interest_usdt)), 'null')),
            array_construct('revised_keys', 'exact', coalesce(to_varchar(count_if(_is_revised)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__OPEN_INTEREST_1H
) as s, lateral flatten(input => s.a) as m

union all

select 11 as ord, 'staging' as layer, 'stg_binance__long_short_ratio_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct contract_symbol, ratio_series, snapshot_time_ms)), 'null')),
            array_construct('revised_keys', 'exact', coalesce(to_varchar(count_if(_is_revised)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__LONG_SHORT_RATIO_1H
) as s, lateral flatten(input => s.a) as m

union all

select 12 as ord, 'staging' as layer, 'stg_binance__long_short_ratio_1h' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select ratio_series::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('contracts', 'exact', coalesce(to_varchar(count(distinct contract_symbol)), 'null')),
            array_construct('min_snapshot_time_ms', 'exact', coalesce(to_varchar(min(snapshot_time_ms)), 'null')),
            array_construct('max_snapshot_time_ms', 'exact', coalesce(to_varchar(max(snapshot_time_ms)), 'null')),
            array_construct('sum_long_share', 'exact', coalesce(to_varchar(sum(long_share)), 'null')),
            array_construct('sum_short_share', 'exact', coalesce(to_varchar(sum(short_share)), 'null')),
            array_construct('sum_long_short_ratio', 'exact', coalesce(to_varchar(sum(long_short_ratio)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.STG_BINANCE__LONG_SHORT_RATIO_1H
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 13 as ord, 'staging' as layer, 'seed symbols' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.SYMBOLS
) as s, lateral flatten(input => s.a) as m

union all

select 14 as ord, 'staging' as layer, 'seed perpetuals' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.PERPETUALS
) as s, lateral flatten(input => s.a) as m

union all

select 15 as ord, 'intermediate' as layer, 'int_binance__hourly_returns' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, open_time_ms)), 'null')),
            array_construct('count_return_1h', 'exact', coalesce(to_varchar(count(return_1h)), 'null')),
            array_construct('sum_return_1h', 'tolerance', coalesce(to_varchar(round(sum(return_1h), 9)::number(38, 9)), 'null')),
            array_construct('pump_hours_5pct', 'exact', coalesce(to_varchar(count_if(return_1h >= 0.05)), 'null')),
            array_construct('count_return_zscore', 'exact', coalesce(to_varchar(count(return_zscore)), 'null')),
            array_construct('sum_return_zscore', 'tolerance', coalesce(to_varchar(round(sum(return_zscore), 6)::number(38, 6)), 'null')),
            array_construct('count_quote_volume_baseline', 'exact', coalesce(to_varchar(count(quote_volume_baseline)), 'null')),
            array_construct('sum_quote_volume_baseline', 'tolerance', coalesce(to_varchar(round(sum(quote_volume_baseline), 4)::number(38, 4)), 'null')),
            array_construct('count_log_volume_zscore', 'exact', coalesce(to_varchar(count(log_volume_zscore)), 'null')),
            array_construct('sum_log_volume_zscore', 'tolerance', coalesce(to_varchar(round(sum(log_volume_zscore), 6)::number(38, 6)), 'null')),
            array_construct('log_volume_zscore_above_5', 'exact', coalesce(to_varchar(count_if(log_volume_zscore > 5)), 'null')),
            array_construct('count_taker_buy_share', 'exact', coalesce(to_varchar(count(taker_buy_share)), 'null')),
            array_construct('sum_taker_buy_share', 'tolerance', coalesce(to_varchar(round(sum(taker_buy_share), 9)::number(38, 9)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.INT_BINANCE__HOURLY_RETURNS
) as s, lateral flatten(input => s.a) as m

union all

select 16 as ord, 'intermediate' as layer, 'int_binance__hourly_returns' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select symbol::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('count_return_1h', 'exact', coalesce(to_varchar(count(return_1h)), 'null')),
            array_construct('pump_hours_5pct', 'exact', coalesce(to_varchar(count_if(return_1h >= 0.05)), 'null')),
            array_construct('count_return_zscore', 'exact', coalesce(to_varchar(count(return_zscore)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.INT_BINANCE__HOURLY_RETURNS
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 17 as ord, 'intermediate' as layer, 'int_binance__hourly_market' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('distinct_keys', 'exact', coalesce(to_varchar(count(distinct symbol, open_time_ms)), 'null')),
            array_construct('count_open_interest_coins', 'exact', coalesce(to_varchar(count(open_interest_coins)), 'null')),
            array_construct('sum_open_interest_coins', 'exact', coalesce(to_varchar(sum(open_interest_coins)), 'null')),
            array_construct('count_funding_rate', 'exact', coalesce(to_varchar(count(funding_rate)), 'null')),
            array_construct('sum_funding_rate', 'exact', coalesce(to_varchar(sum(funding_rate)), 'null')),
            array_construct('sum_funding_rate_8h', 'tolerance', coalesce(to_varchar(round(sum(funding_rate_8h), 12)::number(38, 12)), 'null')),
            array_construct('sum_funding_premium_8h', 'tolerance', coalesce(to_varchar(round(sum(funding_premium_8h), 12)::number(38, 12)), 'null')),
            array_construct('funding_stale_hours', 'exact', coalesce(to_varchar(count_if(is_funding_stale)), 'null')),
            array_construct('count_top_account_ratio', 'exact', coalesce(to_varchar(count(top_account_long_short_ratio)), 'null')),
            array_construct('sum_top_account_ratio', 'exact', coalesce(to_varchar(sum(top_account_long_short_ratio)), 'null')),
            array_construct('count_top_position_ratio', 'exact', coalesce(to_varchar(count(top_position_long_short_ratio)), 'null')),
            array_construct('sum_top_position_ratio', 'exact', coalesce(to_varchar(sum(top_position_long_short_ratio)), 'null')),
            array_construct('count_global_account_ratio', 'exact', coalesce(to_varchar(count(global_account_long_short_ratio)), 'null')),
            array_construct('sum_global_account_ratio', 'exact', coalesce(to_varchar(sum(global_account_long_short_ratio)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.INT_BINANCE__HOURLY_MARKET
) as s, lateral flatten(input => s.a) as m

union all

select 18 as ord, 'intermediate' as layer, 'int_binance__hourly_market' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select symbol::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('count_open_interest_coins', 'exact', coalesce(to_varchar(count(open_interest_coins)), 'null')),
            array_construct('count_funding_rate', 'exact', coalesce(to_varchar(count(funding_rate)), 'null')),
            array_construct('count_top_position_ratio', 'exact', coalesce(to_varchar(count(top_position_long_short_ratio)), 'null')),
            array_construct('funding_stale_hours', 'exact', coalesce(to_varchar(count_if(is_funding_stale)), 'null'))
        ) as a
        from CRYPTO_DWH.STAGING.INT_BINANCE__HOURLY_MARKET
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 19 as ord, 'marts' as layer, 'dim_symbol' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('has_perpetual', 'exact', coalesce(to_varchar(count_if(has_perpetual)), 'null')),
            array_construct('sum_contract_multiplier', 'exact', coalesce(to_varchar(sum(contract_multiplier)), 'null')),
            array_construct('sum_interest_rate_8h', 'exact', coalesce(to_varchar(sum(interest_rate_8h)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.DIM_SYMBOL
) as s, lateral flatten(input => s.a) as m

union all

select 20 as ord, 'marts' as layer, 'fct_pump_events' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('symbols', 'exact', coalesce(to_varchar(count(distinct symbol)), 'null')),
            array_construct('days', 'exact', coalesce(to_varchar(count(distinct to_date(event_hour))), 'null')),
            array_construct('min_event_open_time_ms', 'exact', coalesce(to_varchar(min(event_open_time_ms)), 'null')),
            array_construct('max_event_open_time_ms', 'exact', coalesce(to_varchar(max(event_open_time_ms)), 'null')),
            array_construct('sum_event_open_time_ms', 'exact', coalesce(to_varchar(sum(event_open_time_ms)), 'null')),
            array_construct('min_pump_event_id', 'exact', coalesce(to_varchar(min(pump_event_id)), 'null')),
            array_construct('max_pump_event_id', 'exact', coalesce(to_varchar(max(pump_event_id)), 'null')),
            array_construct('sum_episode_hours', 'exact', coalesce(to_varchar(sum(episode_hours)), 'null')),
            array_construct('sum_market_breadth', 'exact', coalesce(to_varchar(sum(market_breadth)), 'null')),
            array_construct('overlaps_previous_event', 'exact', coalesce(to_varchar(count_if(overlaps_previous_event)), 'null')),
            array_construct('has_full_before_window', 'exact', coalesce(to_varchar(count_if(has_full_before_window)), 'null')),
            array_construct('has_full_after_window', 'exact', coalesce(to_varchar(count_if(has_full_after_window)), 'null')),
            array_construct('has_full_open_interest_window', 'exact', coalesce(to_varchar(count_if(has_full_open_interest_window)), 'null')),
            array_construct('sum_return_1h', 'tolerance', coalesce(to_varchar(round(sum(return_1h), 9)::number(38, 9)), 'null')),
            array_construct('count_return_before', 'exact', coalesce(to_varchar(count(return_before)), 'null')),
            array_construct('sum_return_before', 'tolerance', coalesce(to_varchar(round(sum(return_before), 9)::number(38, 9)), 'null')),
            array_construct('count_return_after', 'exact', coalesce(to_varchar(count(return_after)), 'null')),
            array_construct('sum_return_after', 'tolerance', coalesce(to_varchar(round(sum(return_after), 9)::number(38, 9)), 'null')),
            array_construct('count_volume_ratio_event', 'exact', coalesce(to_varchar(count(volume_ratio_event)), 'null')),
            array_construct('sum_volume_ratio_event', 'tolerance', coalesce(to_varchar(round(sum(volume_ratio_event), 6)::number(38, 6)), 'null')),
            array_construct('count_open_interest_change_event', 'exact', coalesce(to_varchar(count(open_interest_change_event)), 'null')),
            array_construct('sum_open_interest_change_event', 'tolerance', coalesce(to_varchar(round(sum(open_interest_change_event), 9)::number(38, 9)), 'null')),
            array_construct('count_funding_rate_8h_before', 'exact', coalesce(to_varchar(count(funding_rate_8h_before)), 'null')),
            array_construct('sum_funding_rate_8h_before', 'tolerance', coalesce(to_varchar(round(sum(funding_rate_8h_before), 12)::number(38, 12)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS
) as s, lateral flatten(input => s.a) as m

union all

select 21 as ord, 'marts' as layer, 'fct_pump_events' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select event_scope::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 22 as ord, 'marts' as layer, 'fct_pump_events' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select symbol::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('sum_event_open_time_ms', 'exact', coalesce(to_varchar(sum(event_open_time_ms)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENTS
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 23 as ord, 'marts' as layer, 'fct_pump_event_window' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('events', 'exact', coalesce(to_varchar(count(distinct pump_event_id)), 'null')),
            array_construct('count_return_since_anchor_close', 'exact', coalesce(to_varchar(count(return_since_anchor_close)), 'null')),
            array_construct('sum_return_since_anchor_close', 'tolerance', coalesce(to_varchar(round(sum(return_since_anchor_close), 9)::number(38, 9)), 'null')),
            array_construct('count_volume_ratio', 'exact', coalesce(to_varchar(count(volume_ratio)), 'null')),
            array_construct('sum_volume_ratio', 'tolerance', coalesce(to_varchar(round(sum(volume_ratio), 6)::number(38, 6)), 'null')),
            array_construct('count_taker_buy_share', 'exact', coalesce(to_varchar(count(taker_buy_share)), 'null')),
            array_construct('sum_taker_buy_share', 'tolerance', coalesce(to_varchar(round(sum(taker_buy_share), 9)::number(38, 9)), 'null')),
            array_construct('count_open_interest_change_since_start', 'exact', coalesce(to_varchar(count(open_interest_change_since_start)), 'null')),
            array_construct('count_top_position_ratio_change', 'exact', coalesce(to_varchar(count(top_position_ratio_change_since_start)), 'null')),
            array_construct('sum_top_position_ratio_change', 'tolerance', coalesce(to_varchar(round(sum(top_position_ratio_change_since_start), 9)::number(38, 9)), 'null')),
            array_construct('funding_stale_rows', 'exact', coalesce(to_varchar(count_if(is_funding_stale)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENT_WINDOW
) as s, lateral flatten(input => s.a) as m

union all

select 24 as ord, 'marts' as layer, 'fct_pump_control_window' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'engine_dependent', coalesce(to_varchar(count(*)), 'null')),
            array_construct('events', 'exact', coalesce(to_varchar(count(distinct pump_event_id)), 'null')),
            array_construct('year_control_rows', 'exact', coalesce(to_varchar(count_if(is_year_control)), 'null')),
            array_construct('recent_control_rows', 'exact', coalesce(to_varchar(count_if(is_recent_control)), 'null')),
            array_construct('distinct_control_hours', 'engine_dependent', coalesce(to_varchar(count(distinct pump_event_id, control_open_time_ms)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_CONTROL_WINDOW
) as s, lateral flatten(input => s.a) as m

union all

select 25 as ord, 'marts' as layer, 'fct_pump_event_excess' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENT_EXCESS
) as s, lateral flatten(input => s.a) as m

union all

select 26 as ord, 'marts' as layer, 'fct_pump_event_excess' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select metric::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('count_value', 'exact', coalesce(to_varchar(count(value)), 'null')),
            array_construct('sum_value', 'tolerance', coalesce(to_varchar(round(sum(value), 9)::number(38, 9)), 'null')),
            array_construct('sum_control_hours', 'engine_dependent', coalesce(to_varchar(sum(control_hours)), 'null')),
            array_construct('sum_excess', 'engine_dependent', coalesce(to_varchar(round(sum(excess), 9)::number(38, 9)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.FCT_PUMP_EVENT_EXCESS
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 27 as ord, 'marts' as layer, 'mart_pump_profile' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.MART_PUMP_PROFILE
) as s, lateral flatten(input => s.a) as m

union all

select 28 as ord, 'marts' as layer, 'mart_pump_profile' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select metric::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('sum_events', 'exact', coalesce(to_varchar(sum(events)), 'null')),
            array_construct('sum_event_median', 'tolerance', coalesce(to_varchar(round(sum(event_median), 9)::number(38, 9)), 'null')),
            array_construct('sum_control_median', 'engine_dependent', coalesce(to_varchar(round(sum(control_median), 9)::number(38, 9)), 'null')),
            array_construct('sum_median_excess', 'engine_dependent', coalesce(to_varchar(round(sum(median_excess), 9)::number(38, 9)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.MART_PUMP_PROFILE
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 29 as ord, 'marts' as layer, 'mart_pump_signal_check' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('table_hash', 'snowflake_only', coalesce(to_varchar(hash_agg(*)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.MART_PUMP_SIGNAL_CHECK
) as s, lateral flatten(input => s.a) as m

union all

select 30 as ord, 'marts' as layer, 'mart_pump_signal_check' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select rule::varchar as grain, array_construct(
            array_construct('days', 'exact', coalesce(to_varchar(count(*)), 'null')),
            array_construct('sum_signal_hours', 'exact', coalesce(to_varchar(sum(signal_hours)), 'null')),
            array_construct('sum_followed_by_pump', 'exact', coalesce(to_varchar(sum(followed_by_pump)), 'null'))
        ) as a
        from CRYPTO_DWH.MARTS.MART_PUMP_SIGNAL_CHECK
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 31 as ord, 'dq' as layer, 'dq_data_coverage' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select dataset::varchar as grain, array_construct(
            array_construct('row_count', 'exact', coalesce(to_varchar(max(row_count)), 'null')),
            array_construct('symbol_count', 'exact', coalesce(to_varchar(max(symbol_count)), 'null')),
            array_construct('data_from_utc', 'exact', coalesce(to_varchar(to_varchar(max(data_from), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')), 'null')),
            array_construct('data_to_utc', 'exact', coalesce(to_varchar(to_varchar(max(data_to), 'YYYY-MM-DD"T"HH24:MI:SS"Z"')), 'null')),
            array_construct('latest_batch_id', 'exact', coalesce(to_varchar(max(latest_batch_id)), 'null'))
        ) as a
        from CRYPTO_DWH.DQ.DQ_DATA_COVERAGE
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 32 as ord, 'dq' as layer, 'dq_check_results (last build and last test)' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select dbt_command || ' ' || check_type || ' ' || severity || ' ' || status::varchar as grain, array_construct(
            array_construct('checks', 'reference', coalesce(to_varchar(count(*)), 'null')),
            array_construct('sum_failures', 'reference', coalesce(to_varchar(coalesce(sum(failures), 0)), 'null'))
        ) as a
        from last_invocation_results
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 33 as ord, 'dq' as layer, 'dq_check_results (last build and last test)' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select check_name::varchar as grain, array_construct(
            array_construct('status', 'exact', coalesce(to_varchar(max(status)), 'null')),
            array_construct('failures', 'exact', coalesce(to_varchar(max(failures)), 'null'))
        ) as a
        from last_invocation_results where lower(status) <> 'pass'
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 34 as ord, 'dq' as layer, 'findings tables (store_failures_as table)' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select lower(table_name)::varchar as grain, array_construct(
            array_construct('row_count', 'exact', coalesce(to_varchar(max(row_count)), 'null'))
        ) as a
        from CRYPTO_DWH.INFORMATION_SCHEMA.TABLES where table_schema = 'DQ' and table_type = 'BASE TABLE' and table_name like 'DQ%' 
        group by 1
) as s, lateral flatten(input => s.a) as m

union all

select 35 as ord, 'dq' as layer, 'dbt_node_results (journal)' as relation, s.grain,
    m.index as metric_ord, m.value[0]::varchar as metric,
    m.value[1]::varchar as compare, m.value[2]::varchar as value
from (
    select 'all'::varchar as grain, array_construct(
            array_construct('rows', 'snowflake_only', coalesce(to_varchar(count(*)), 'null')),
            array_construct('invocations', 'snowflake_only', coalesce(to_varchar(count(distinct invocation_id)), 'null')),
            array_construct('first_run_started_at', 'snowflake_only', coalesce(to_varchar(to_varchar(min(run_started_at))), 'null')),
            array_construct('last_run_started_at', 'snowflake_only', coalesce(to_varchar(to_varchar(max(run_started_at))), 'null'))
        ) as a
        from CRYPTO_DWH.DQ.DBT_NODE_RESULTS
) as s, lateral flatten(input => s.a) as m
)

select layer, relation, grain, metric, compare, value
from metrics
order by ord, grain, metric_ord;
