-- =============================================================================
-- crypto-market-dwh: one-time Snowflake account setup
--
-- Run in a Snowsight worksheet as a user that has ACCOUNTADMIN ("Run All").
-- Idempotent: re-running changes nothing that is already in the desired state.
--   * CREATE ... IF NOT EXISTS creates an object once and never drops it.
--   * IF NOT EXISTS skips the whole statement when the object exists, so the
--     properties we care about are (re)applied with ALTER ... SET. The ALTER is
--     the single source of truth: edit it and re-run to converge.
--   * No CREATE OR REPLACE: it is DROP + CREATE. For a resource monitor that
--     resets used credits; for a warehouse it aborts running queries.
--
-- Objects:
--   ACCOUNT_TRIAL_MONITOR   account-wide credit cap for the whole trial
--   CRYPTO_WH_DAILY_MONITOR daily credit cap for the project warehouse
--   CRYPTO_DWH_ROLE         project role (loader + dbt); owns tables and views
--   CRYPTO_WH               X-Small warehouse, suspends after 60 s idle
--   CRYPTO_DWH              database with schemas RAW, STAGING, MARTS
--   CRYPTO_LOADER           service user for ingestion, project role only
-- =============================================================================

-- The user running this script gets the project role (see section 2).
SET setup_user = CURRENT_USER();


-- -----------------------------------------------------------------------------
-- 1. Cost control (ACCOUNTADMIN only can create and assign resource monitors)
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Safety net for every warehouse in the account, including ones created later.
-- FREQUENCY = NEVER: usage never resets, the quota covers the whole trial.
CREATE RESOURCE MONITOR IF NOT EXISTS ACCOUNT_TRIAL_MONITOR
    WITH FREQUENCY = NEVER
    START_TIMESTAMP = IMMEDIATELY;

-- Suspend below 100 %: warehouses take time to stop, usage can overshoot.
ALTER RESOURCE MONITOR ACCOUNT_TRIAL_MONITOR
    SET CREDIT_QUOTA = 30
    TRIGGERS
        ON 50 PERCENT DO NOTIFY
        ON 80 PERCENT DO NOTIFY
        ON 90 PERCENT DO SUSPEND
        ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER ACCOUNT SET RESOURCE_MONITOR = ACCOUNT_TRIAL_MONITOR;

-- Catches a runaway job within a day (e.g. a retry loop or an Airflow
-- backfill gone wrong). Usage resets at 00:00 UTC.
CREATE RESOURCE MONITOR IF NOT EXISTS CRYPTO_WH_DAILY_MONITOR
    WITH FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY;

ALTER RESOURCE MONITOR CRYPTO_WH_DAILY_MONITOR
    SET CREDIT_QUOTA = 2
    TRIGGERS
        ON 75 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND
        ON 110 PERCENT DO SUSPEND_IMMEDIATE;


-- -----------------------------------------------------------------------------
-- 2. Project role (USERADMIN manages roles; not ACCOUNTADMIN for daily work)
-- -----------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS CRYPTO_DWH_ROLE
    COMMENT = 'crypto-market-dwh: ingestion and dbt';

-- Role hierarchy: SYSADMIN inherits the project role, so admins can manage
-- every object the project role creates.
GRANT ROLE CRYPTO_DWH_ROLE TO ROLE SYSADMIN;
GRANT ROLE CRYPTO_DWH_ROLE TO USER IDENTIFIER($setup_user);


-- -----------------------------------------------------------------------------
-- 3. Warehouse, database, schemas, grants (SYSADMIN owns the containers)
-- -----------------------------------------------------------------------------
USE ROLE SYSADMIN;

-- Created suspended: creating it costs nothing.
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
    WAREHOUSE_SIZE = XSMALL
    INITIALLY_SUSPENDED = TRUE;

ALTER WAREHOUSE CRYPTO_WH SET
    WAREHOUSE_SIZE = XSMALL
    -- Gen1: 1 credit/h for X-Small. Gen2 (the default for new warehouses)
    -- costs 1.35x on AWS; short jobs are dominated by the 60 s billing minimum.
    GENERATION = '1'
    AUTO_SUSPEND = 60           -- seconds idle before suspend (default 600)
    AUTO_RESUME = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 600  -- default is 2 days of credits
    COMMENT = 'crypto-market-dwh: ingestion and dbt';

CREATE DATABASE IF NOT EXISTS CRYPTO_DWH
    COMMENT = 'crypto-market-dwh: Binance market data';

CREATE SCHEMA IF NOT EXISTS CRYPTO_DWH.RAW
    COMMENT = 'Source data as received from APIs, plus load metadata';
CREATE SCHEMA IF NOT EXISTS CRYPTO_DWH.STAGING
    COMMENT = 'dbt staging: typed, deduplicated, UTC';
CREATE SCHEMA IF NOT EXISTS CRYPTO_DWH.MARTS
    COMMENT = 'dbt marts: facts and dimensions for analysis';

-- GRANT is idempotent: granting an existing privilege is a no-op.
GRANT USAGE, OPERATE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_DWH_ROLE;
GRANT USAGE ON DATABASE CRYPTO_DWH TO ROLE CRYPTO_DWH_ROLE;

-- The project role creates objects inside the schemas (and owns them),
-- but cannot drop or rename the schemas themselves. Caveat: a user's other
-- roles are active as secondary roles by default (DEFAULT_SECONDARY_ROLES =
-- ('ALL')), so least privilege holds only for a user granted this role alone.
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA CRYPTO_DWH.RAW TO ROLE CRYPTO_DWH_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.STAGING TO ROLE CRYPTO_DWH_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.MARTS TO ROLE CRYPTO_DWH_ROLE;


-- -----------------------------------------------------------------------------
-- 4. Attach the monitor, user defaults, default warehouses (ACCOUNTADMIN)
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

ALTER WAREHOUSE CRYPTO_WH SET RESOURCE_MONITOR = CRYPTO_WH_DAILY_MONITOR;

-- Daily work runs as the project role, not ACCOUNTADMIN.
ALTER USER IDENTIFIER($setup_user) SET
    DEFAULT_ROLE = CRYPTO_DWH_ROLE
    DEFAULT_WAREHOUSE = CRYPTO_WH
    DEFAULT_NAMESPACE = CRYPTO_DWH.RAW;

-- Warehouses Snowflake provisions in a new trial account (Gen2, owned by
-- ACCOUNTADMIN, 300 s idle): same idle and timeout limits. Not dropped:
-- Snowsight learning and notebook features rely on them. IF EXISTS keeps the
-- script valid on accounts that do not have them.
ALTER WAREHOUSE IF EXISTS COMPUTE_WH SET
    AUTO_SUSPEND = 60
    STATEMENT_TIMEOUT_IN_SECONDS = 600;
ALTER WAREHOUSE IF EXISTS SNOWFLAKE_LEARNING_WH SET
    AUTO_SUSPEND = 60
    STATEMENT_TIMEOUT_IN_SECONDS = 600;


-- -----------------------------------------------------------------------------
-- 5. Service user for the loader (USERADMIN creates and owns users)
-- -----------------------------------------------------------------------------
-- TYPE = SERVICE: no password, no MFA; authenticates with a key pair only.
-- Granted the project role alone, so secondary roles cannot widen its rights.
-- The public key is registered separately and is not part of this script:
--   ALTER USER CRYPTO_LOADER ADD KEY PAIR <key_name> PUBLIC_KEY = '<base64>';
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS CRYPTO_LOADER
    TYPE = SERVICE
    COMMENT = 'crypto-market-dwh: ingestion (key-pair auth)';

ALTER USER CRYPTO_LOADER SET
    DEFAULT_ROLE = CRYPTO_DWH_ROLE
    DEFAULT_WAREHOUSE = CRYPTO_WH
    DEFAULT_NAMESPACE = CRYPTO_DWH.RAW
    DEFAULT_SECONDARY_ROLES = ()
    COMMENT = 'crypto-market-dwh: ingestion (key-pair auth)';

GRANT ROLE CRYPTO_DWH_ROLE TO USER CRYPTO_LOADER;
