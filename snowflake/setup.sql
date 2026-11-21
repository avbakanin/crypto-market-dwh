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
--   * REVOKE succeeds even when there is nothing to revoke, so taking a
--     privilege away is as idempotent as granting it.
--
-- Objects:
--   ACCOUNT_TRIAL_MONITOR   account-wide credit cap for the whole trial
--   CRYPTO_WH_DAILY_MONITOR daily credit cap for the project warehouse
--   CRYPTO_DWH_ROLE         ingestion role: writes RAW only, owns the RAW objects
--   CRYPTO_TRANSFORMER      dbt role: reads RAW, owns the objects in STAGING, MARTS, DQ
--   CRYPTO_WH               X-Small warehouse, suspends after 60 s idle
--   CRYPTO_DWH              database with schemas RAW, STAGING, MARTS, DQ
--   CRYPTO_LOADER           service user for ingestion, CRYPTO_DWH_ROLE only
--   CRYPTO_DBT              service user for dbt, CRYPTO_TRANSFORMER only
-- =============================================================================

-- The user running this script gets both project roles (see section 2).
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
-- 2. Project roles (USERADMIN manages roles; not ACCOUNTADMIN for daily work)
-- -----------------------------------------------------------------------------
USE ROLE USERADMIN;

-- One role per job: the loader cannot touch dbt's schemas, dbt cannot write RAW.
CREATE ROLE IF NOT EXISTS CRYPTO_DWH_ROLE;
ALTER ROLE CRYPTO_DWH_ROLE SET
    COMMENT = 'crypto-market-dwh: ingestion, writes RAW';

CREATE ROLE IF NOT EXISTS CRYPTO_TRANSFORMER;
ALTER ROLE CRYPTO_TRANSFORMER SET
    COMMENT = 'crypto-market-dwh: dbt, reads RAW, builds STAGING, MARTS and DQ';

-- Role hierarchy: SYSADMIN inherits the project roles, so admins can manage
-- every object they create.
GRANT ROLE CRYPTO_DWH_ROLE TO ROLE SYSADMIN;
GRANT ROLE CRYPTO_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE CRYPTO_DWH_ROLE TO USER IDENTIFIER($setup_user);
GRANT ROLE CRYPTO_TRANSFORMER TO USER IDENTIFIER($setup_user);


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
CREATE SCHEMA IF NOT EXISTS CRYPTO_DWH.DQ
    COMMENT = 'dbt data quality: failing rows of tests and the log of test results';

-- GRANT is idempotent: granting an existing privilege is a no-op.
-- USAGE alone lets a query auto-resume the warehouse; OPERATE (explicit
-- RESUME/SUSPEND) is needed by neither the loader nor dbt. The loader had it
-- until week 4: taken back, REVOKE converges like GRANT.
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_DWH_ROLE;
REVOKE OPERATE ON WAREHOUSE CRYPTO_WH FROM ROLE CRYPTO_DWH_ROLE;
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_TRANSFORMER;
GRANT USAGE ON DATABASE CRYPTO_DWH TO ROLE CRYPTO_DWH_ROLE;
GRANT USAGE ON DATABASE CRYPTO_DWH TO ROLE CRYPTO_TRANSFORMER;

-- The project roles create objects inside the schemas (and own them), but
-- cannot drop or rename the schemas themselves. Caveat: a user's other roles
-- are active as secondary roles by default (DEFAULT_SECONDARY_ROLES = ('ALL')),
-- so least privilege holds only for a user granted one role alone.

-- Ingestion: the loader creates stages, file formats and tables in RAW only.
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FILE FORMAT
    ON SCHEMA CRYPTO_DWH.RAW TO ROLE CRYPTO_DWH_ROLE;
-- Taken back from the time one role did both jobs: the loader never creates
-- views, and STAGING, MARTS and DQ belong to dbt.
REVOKE CREATE VIEW ON SCHEMA CRYPTO_DWH.RAW FROM ROLE CRYPTO_DWH_ROLE;
REVOKE USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.STAGING FROM ROLE CRYPTO_DWH_ROLE;
REVOKE USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.MARTS FROM ROLE CRYPTO_DWH_ROLE;
REVOKE USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.DQ FROM ROLE CRYPTO_DWH_ROLE;

-- dbt: builds tables and views (models, seeds) in STAGING and MARTS; in DQ it
-- stores the failing rows of tests (store_failures) and appends to its log of
-- test results (on-run-end hook).
-- Reading RAW is granted in section 6: it needs MANAGE GRANTS.
GRANT USAGE ON SCHEMA CRYPTO_DWH.RAW TO ROLE CRYPTO_TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.STAGING TO ROLE CRYPTO_TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.MARTS TO ROLE CRYPTO_TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA CRYPTO_DWH.DQ TO ROLE CRYPTO_TRANSFORMER;


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

-- Trial accounts grant SNOWFLAKE_LEARNING_ROLE to PUBLIC, i.e. to every user,
-- service users included: USAGE/OPERATE on SNOWFLAKE_LEARNING_WH (Gen2, outside
-- the daily monitor) and CREATE SCHEMA in SNOWFLAKE_LEARNING_DB. Keep it for
-- the admin (Snowsight learning features), take it away from everyone else.
-- Fails loudly on accounts without this role: delete these two lines there.
GRANT ROLE SNOWFLAKE_LEARNING_ROLE TO USER IDENTIFIER($setup_user);
REVOKE ROLE SNOWFLAKE_LEARNING_ROLE FROM ROLE PUBLIC;


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


-- -----------------------------------------------------------------------------
-- 6. dbt reads RAW (SECURITYADMIN: grants on future objects need MANAGE GRANTS)
-- -----------------------------------------------------------------------------
-- The loader role owns the RAW tables and creates new ones at the start of a
-- run (raw_*.sql). ALL covers the tables that exist now, FUTURE every table the
-- loader creates later; neither lets dbt write, stage or load files.
-- A schema-level future grant overrides database-level ones for this schema.
USE ROLE SECURITYADMIN;

GRANT SELECT ON ALL TABLES IN SCHEMA CRYPTO_DWH.RAW TO ROLE CRYPTO_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CRYPTO_DWH.RAW TO ROLE CRYPTO_TRANSFORMER;


-- -----------------------------------------------------------------------------
-- 7. Service user for dbt (USERADMIN creates and owns users)
-- -----------------------------------------------------------------------------
-- Same pattern as CRYPTO_LOADER: key pair only, one role, no secondary roles.
-- TIMEZONE = 'UTC': a dbt profile cannot set session parameters other than
-- QUERY_TAG, and the account default is America/Los_Angeles; models also
-- convert to UTC explicitly, this is the second barrier.
-- The public key is registered separately and is not part of this script:
--   ALTER USER CRYPTO_DBT ADD KEY PAIR <key_name> PUBLIC_KEY = '<base64>';
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS CRYPTO_DBT
    TYPE = SERVICE
    COMMENT = 'crypto-market-dwh: dbt (key-pair auth)';

ALTER USER CRYPTO_DBT SET
    DEFAULT_ROLE = CRYPTO_TRANSFORMER
    DEFAULT_WAREHOUSE = CRYPTO_WH
    DEFAULT_NAMESPACE = CRYPTO_DWH.STAGING
    DEFAULT_SECONDARY_ROLES = ()
    TIMEZONE = 'UTC'
    COMMENT = 'crypto-market-dwh: dbt (key-pair auth)';

GRANT ROLE CRYPTO_TRANSFORMER TO USER CRYPTO_DBT;
