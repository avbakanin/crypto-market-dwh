{{ config(tags=['dq_log']) }}

-- The DQ log against the findings tables. For every test that stores its failing rows as
-- a table: the table exists, its row count equals the failures its last executed run
-- logged, and that log row was written after the table (a hook that stopped logging
-- leaves older rows that would still match). Both times are Snowflake's clock.
-- Runs after the build has written the log, as a second step:
--   dbt test --select tag:dq_log
-- (selectors.yml leaves it out of a plain dbt build: tests of a build run before its
-- on-run-end hook, so it would compare new tables with the previous run's log.)

{% set stored = [] %}
{% if execute %}
    {% for node in graph.nodes.values()
        if node.resource_type == 'test' and node.config.store_failures_as == 'table' %}
        {% do stored.append(node) %}
    {% endfor %}
{% endif %}

with stored_tests as (

    {% for node in stored %}
    select
        '{{ node.unique_id }}' as check_unique_id,
        '{{ node.schema | upper }}' as table_schema,
        '{{ node.alias | upper }}' as table_name
    {% if not loop.last %}union all{% endif %}
    {% else %}
    select
        null::varchar as check_unique_id,
        null::varchar as table_schema,
        null::varchar as table_name
    where false
    {% endfor %}

),

-- row_count and created: metadata, exact for permanent tables; dbt drops and recreates
-- a findings table on every run of its test.
dq_tables as (

    select table_schema, table_name, row_count, created
    from {{ target.database }}.information_schema.tables
    where table_type = 'BASE TABLE'

),

-- Skipped and errored runs leave the table of the last run that executed the test.
last_executed as (

    select check_unique_id, status, failures, logged_at
    from {{ ref('dq_check_results') }}
    where status in ('pass', 'warn', 'fail')
    qualify row_number() over (
        partition by check_unique_id order by run_started_at desc, invocation_id desc
    ) = 1

)

select
    stored_tests.check_unique_id,
    stored_tests.table_name,
    dq_tables.row_count as stored_rows,
    dq_tables.created as table_created_at,
    last_executed.status as logged_status,
    last_executed.failures as logged_failures,
    last_executed.logged_at,
    case
        when dq_tables.table_name is null then 'findings table missing'
        when last_executed.check_unique_id is null then 'test not in the log'
        when last_executed.logged_at < dq_tables.created then 'log older than the table'
        else 'row count differs from the log'
    end as issue
from stored_tests
left join dq_tables
    on stored_tests.table_schema = dq_tables.table_schema
    and stored_tests.table_name = dq_tables.table_name
left join last_executed
    on stored_tests.check_unique_id = last_executed.check_unique_id
where dq_tables.table_name is null
    or last_executed.check_unique_id is null
    or last_executed.logged_at < dq_tables.created
    or last_executed.failures is distinct from dq_tables.row_count
