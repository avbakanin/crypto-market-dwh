-- =============================================================================
-- crypto-market-dwh: RAW objects for Binance spot klines
--
-- Executed by the loader at the start of every run (python -m ingestion.load_raw_klines)
-- as CRYPTO_LOADER with CRYPTO_DWH_ROLE, so the project role owns these objects.
-- Every statement is metadata-only: running it does not resume the warehouse.
-- Manual run in Snowsight: USE ROLE CRYPTO_DWH_ROLE; USE DATABASE CRYPTO_DWH; then
-- Run All. The primary role becomes the owner, whatever the secondary roles are.
--
-- Idempotent, same pattern as setup.sql: CREATE ... IF NOT EXISTS creates once,
-- ALTER ... SET re-applies the options we rely on.
-- No CREATE OR REPLACE on the table: it would drop the loaded rows together with
-- COPY's load metadata (the per-table memory of loaded files that prevents reloads).
-- A column change goes in as an explicit ALTER TABLE below the CREATE.
--
-- Objects (schema RAW of the session's database):
--   KLINES_JSON_FORMAT       JSON, outer array stripped: one row per kline
--   KLINES_STAGE             internal stage, one directory per extract batch
--   BINANCE_SPOT_KLINES_1H   append-only klines as received, plus load metadata
-- =============================================================================

CREATE FILE FORMAT IF NOT EXISTS RAW.KLINES_JSON_FORMAT
    TYPE = JSON;

-- Each file is one JSON array of klines; without this the whole file is one row.
ALTER FILE FORMAT RAW.KLINES_JSON_FORMAT SET
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'Binance klines files: a JSON array of 12-field arrays';

CREATE STAGE IF NOT EXISTS RAW.KLINES_STAGE;

-- The stage default format lets ad-hoc queries read staged files: SELECT $1 FROM @RAW.KLINES_STAGE
ALTER STAGE RAW.KLINES_STAGE SET
    FILE_FORMAT = (FORMAT_NAME = 'RAW.KLINES_JSON_FORMAT')
    COMMENT = 'Landing zone: <batch_id>/<SYMBOL>.json.gz, files are never overwritten';

CREATE TABLE IF NOT EXISTS RAW.BINANCE_SPOT_KLINES_1H (
    symbol VARCHAR NOT NULL
        COMMENT 'Trading pair from the file name, e.g. BTCUSDT',
    payload VARIANT NOT NULL
        COMMENT 'One kline exactly as received: 12-field array, prices as strings',
    _batch_id VARCHAR NOT NULL
        COMMENT 'Extract run = stage directory: UTC as-of moment, e.g. 20261120T143015Z',
    _source_file VARCHAR NOT NULL
        COMMENT 'METADATA$FILENAME: path of the staged file',
    _file_row_number NUMBER(38, 0) NOT NULL
        COMMENT 'METADATA$FILE_ROW_NUMBER: position of the kline in its file',
    _file_content_key VARCHAR NOT NULL
        COMMENT 'METADATA$FILE_CONTENT_KEY: checksum of the staged file',
    _loaded_at TIMESTAMP_TZ NOT NULL
        COMMENT 'METADATA$START_SCAN_TIME in UTC: when COPY read the file'
);

ALTER TABLE RAW.BINANCE_SPOT_KLINES_1H SET
    COMMENT = 'Binance spot 1h klines, append-only as received; typing and dedup in STAGING';
