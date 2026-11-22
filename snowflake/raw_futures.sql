-- =============================================================================
-- crypto-market-dwh: RAW objects for Binance USD-M futures (funding rate, open interest,
-- long/short ratios)
--
-- Executed by the loader at the start of every run, after raw_klines.sql, as
-- CRYPTO_LOADER with CRYPTO_DWH_ROLE, so the project role owns these objects.
-- Every statement is metadata-only: running it does not resume the warehouse.
-- Manual run in Snowsight: USE ROLE CRYPTO_DWH_ROLE; USE DATABASE CRYPTO_DWH; then
-- Run All. The primary role becomes the owner, whatever the secondary roles are.
--
-- Same pattern as raw_klines.sql: CREATE ... IF NOT EXISTS creates once, ALTER ... SET
-- re-applies the options we rely on, no CREATE OR REPLACE on a table (it would drop the
-- rows and COPY's load metadata).
--
-- Objects (schema RAW of the session's database):
--   JSON_ARRAY_FORMAT                JSON, outer array stripped: one row per record
--   FUNDING_RATE_STAGE               internal stage, one directory per extract batch
--   OPEN_INTEREST_STAGE              internal stage, one directory per extract batch
--   BINANCE_FUTURES_FUNDING_RATE     append-only funding records as received
--   BINANCE_FUTURES_OPEN_INTEREST_1H append-only hourly open interest as received
--   TOP_ACCOUNT_RATIO_STAGE, TOP_POSITION_RATIO_STAGE, GLOBAL_ACCOUNT_RATIO_STAGE
--                                    internal stages of the three long/short series
--   BINANCE_FUTURES_TOP_ACCOUNT_RATIO_1H    append-only hourly long/short ratio of the top
--   BINANCE_FUTURES_TOP_POSITION_RATIO_1H   traders by accounts and by position, and of all
--   BINANCE_FUTURES_GLOBAL_ACCOUNT_RATIO_1H traders by accounts, as received
-- symbol is the futures contract (1000PEPEUSDT, not PEPEUSDT): config/perpetuals.csv maps it
-- to the spot symbol and holds the contract multiplier.
-- =============================================================================

CREATE FILE FORMAT IF NOT EXISTS RAW.JSON_ARRAY_FORMAT
    TYPE = JSON;

-- Each file is one JSON array of records; without this the whole file is one row.
ALTER FILE FORMAT RAW.JSON_ARRAY_FORMAT SET
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'A JSON array of records per file: one row per record';

CREATE STAGE IF NOT EXISTS RAW.FUNDING_RATE_STAGE;

ALTER STAGE RAW.FUNDING_RATE_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_ARRAY_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<CONTRACT>.json.gz, files are never overwritten';

CREATE STAGE IF NOT EXISTS RAW.OPEN_INTEREST_STAGE;

ALTER STAGE RAW.OPEN_INTEREST_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_ARRAY_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<CONTRACT>.json.gz, files are never overwritten';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_FUTURES_FUNDING_RATE (
    symbol VARCHAR NOT NULL
        COMMENT 'USD-M perpetual contract from the file name, e.g. 1000PEPEUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One funding record as received; fundingTime lands ms after the hour',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261121T090749Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the record in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_FUTURES_FUNDING_RATE SET
    COMMENT = 'Binance USD-M funding rate history, append-only; typing and dedup in STAGING';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_FUTURES_OPEN_INTEREST_1H (
    symbol VARCHAR NOT NULL
        COMMENT 'USD-M perpetual contract from the file name, e.g. 1000PEPEUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One hourly open interest point as received, numbers as strings',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261121T093150Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the point in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_FUTURES_OPEN_INTEREST_1H SET
    COMMENT = 'Binance USD-M hourly open interest, append-only; the only copy past a month';

-- Long/short ratios: one stage and one table per series (the payloads have the same shape,
-- so the series is told by the table, never by the record). Served ~30 days, like OI.

CREATE STAGE IF NOT EXISTS RAW.TOP_ACCOUNT_RATIO_STAGE;

ALTER STAGE RAW.TOP_ACCOUNT_RATIO_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_ARRAY_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<CONTRACT>.json.gz, files are never overwritten';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_FUTURES_TOP_ACCOUNT_RATIO_1H (
    symbol VARCHAR NOT NULL
        COMMENT 'USD-M perpetual contract from the file name, e.g. 1000PEPEUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One hourly point as received: longAccount, shortAccount, longShortRatio',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261122T210712Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the point in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_FUTURES_TOP_ACCOUNT_RATIO_1H SET
    COMMENT = 'Top 20% traders by margin balance: shares of accounts long and short, hourly, append-only; the only copy past a month';

CREATE STAGE IF NOT EXISTS RAW.TOP_POSITION_RATIO_STAGE;

ALTER STAGE RAW.TOP_POSITION_RATIO_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_ARRAY_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<CONTRACT>.json.gz, files are never overwritten';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_FUTURES_TOP_POSITION_RATIO_1H (
    symbol VARCHAR NOT NULL
        COMMENT 'USD-M perpetual contract from the file name, e.g. 1000PEPEUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One hourly point as received: longAccount, shortAccount, longShortRatio',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261122T210712Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the point in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_FUTURES_TOP_POSITION_RATIO_1H SET
    COMMENT = 'Top 20% traders by margin balance: shares of their position long and short, hourly, append-only; the only copy past a month';

CREATE STAGE IF NOT EXISTS RAW.GLOBAL_ACCOUNT_RATIO_STAGE;

ALTER STAGE RAW.GLOBAL_ACCOUNT_RATIO_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.JSON_ARRAY_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<CONTRACT>.json.gz, files are never overwritten';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_FUTURES_GLOBAL_ACCOUNT_RATIO_1H (
    symbol VARCHAR NOT NULL
        COMMENT 'USD-M perpetual contract from the file name, e.g. 1000PEPEUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One hourly point as received: longAccount, shortAccount, longShortRatio',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261122T210712Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the point in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_FUTURES_GLOBAL_ACCOUNT_RATIO_1H SET
    COMMENT = 'All traders: shares of accounts long and short, hourly, append-only; the only copy past a month';
