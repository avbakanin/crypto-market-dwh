{#
  DQ log: one row per executed node (model, seed, test, unit test) per dbt invocation.
  Append-only: on-run-start creates the table once, on-run-end appends the results of
  the invocation. The failing rows of each test live in its store_failures relation and
  are replaced on every run; this log keeps the counts over time (ODI: the E$ tables and
  the check journal of the repository).
#}

{% macro dq_log_relation() %}
    {{ return(api.Relation.create(
        database=target.database, schema='DQ', identifier='DBT_NODE_RESULTS')) }}
{% endmacro %}


{# on-run-start. Metadata only: no warehouse. A new column needs an ALTER TABLE. #}
{% macro create_dq_log() %}
create table if not exists {{ dq_log_relation() }} (
    invocation_id varchar not null,
    run_started_at timestamp_tz not null,
    dbt_command varchar not null,
    invocation_command varchar,
    target_name varchar not null,
    dbt_version varchar not null,
    node_unique_id varchar not null,
    resource_type varchar not null,
    node_name varchar not null,
    generic_test varchar,
    tested_node_unique_id varchar,
    depends_on_nodes varchar,
    column_name varchar,
    severity varchar,
    store_failures_as varchar,
    failures_relation varchar,
    status varchar not null,
    failures number(38, 0),
    execution_time_s number(38, 3),
    completed_at timestamp_tz,
    message varchar,
    _logged_at timestamp_tz not null
)
comment = 'dbt results per node and invocation, appended by the on-run-end hook'
{% endmacro %}


{#
  One list of values per result, in the column order of dq_log_columns(). Strings are
  made plain str (~ ''): dbt's statuses and severities are str subclasses, and a
  connector that picks a converter by class name would reject them. Times are UTC.
#}
{% macro dq_log_columns() %}
    {{ return([
        'invocation_id', 'run_started_at', 'dbt_command', 'invocation_command',
        'target_name', 'dbt_version', 'node_unique_id', 'resource_type', 'node_name',
        'generic_test', 'tested_node_unique_id', 'depends_on_nodes', 'column_name',
        'severity', 'store_failures_as', 'failures_relation', 'status', 'failures',
        'execution_time_s', 'completed_at', 'message',
    ]) }}
{% endmacro %}


{% macro dq_log_rows(results) %}
    {% set rows = [] %}
    {% for result in results %}
        {% set node = result.node %}
        {% set is_test = node.resource_type == 'test' %}
        {% set metadata = node.test_metadata if node.test_metadata is defined else none %}
        {% if node.attached_node is defined and node.attached_node %}
            {% set tested = node.attached_node %}
        {% elif node.tested_node_unique_id is defined and node.tested_node_unique_id %}
            {% set tested = node.tested_node_unique_id %}
        {% else %}
            {% set tested = none %}
        {% endif %}
        {# Seeds depend on macros only; sources (dbt source freshness) on nothing. #}
        {% if node.depends_on is defined and node.depends_on.nodes is defined %}
            {% set depends_on = node.depends_on.nodes %}
        {% else %}
            {% set depends_on = [] %}
        {% endif %}
        {% set severity = node.config.get('severity') %}
        {% set stored_as = node.config.get('store_failures_as') if is_test else none %}
        {% set completed_at = result.timing[-1].completed_at if result.timing else none %}
        {% do rows.append([
            invocation_id,
            run_started_at.strftime('%Y-%m-%d %H:%M:%S.%f'),
            flags.WHICH ~ '',
            flags.INVOCATION_COMMAND ~ '' if flags.INVOCATION_COMMAND else none,
            target.name ~ '',
            dbt_version ~ '',
            node.unique_id ~ '',
            node.resource_type ~ '',
            node.name ~ '',
            ((metadata.namespace ~ '.' if metadata.namespace else '') ~ metadata.name) if metadata else none,
            tested,
            depends_on | join(','),
            node.column_name ~ '' if node.column_name is defined and node.column_name else none,
            severity ~ '' if severity else none,
            stored_as ~ '' if stored_as else none,
            node.relation_name ~ '' if stored_as in ['table', 'view'] else none,
            result.status ~ '',
            result.failures,
            result.execution_time | round(3),
            completed_at.strftime('%Y-%m-%d %H:%M:%S.%f') if completed_at else none,
            result.message ~ '' if result.message is not none else none,
        ]) %}
    {% endfor %}
    {{ return(rows) }}
{% endmacro %}


{# INSERT for row_count results, one %s bind variable per value. #}
{% macro dq_log_insert_sql(row_count) %}
    {% set columns = dq_log_columns() %}
    {% set placeholders = (['%s'] * (columns | length)) | join(', ') %}
    {% set insert_sql %}
insert into {{ dq_log_relation() }} ({{ columns | join(', ') }}, _logged_at)
select
    column1,
    to_timestamp_tz(column2 || ' +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'),
    column3, column4, column5, column6, column7, column8, column9, column10, column11,
    column12, column13, column14, column15, column16, column17, column18, column19,
    to_timestamp_tz(column20 || ' +00:00', 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM'),
    column21,
    convert_timezone('UTC', current_timestamp())
from values
{%- for _ in range(row_count) %}
    ({{ placeholders }}){{ ',' if not loop.last }}
{%- endfor %}
    {% endset %}
    {{ return(insert_sql) }}
{% endmacro %}


{#
  on-run-end: appends the results with bind variables (no quoting by hand), 200 rows per
  INSERT. Snowflake autocommits each statement. A failure here fails the dbt invocation:
  a run without its log entry should not look green.
#}
{% macro log_dbt_results(results) %}
    {% if execute and results %}
        {% for chunk in dq_log_rows(results) | batch(200) %}
            {% set bindings = [] %}
            {% for row in chunk %}
                {% do bindings.extend(row) %}
            {% endfor %}
            {% do adapter.add_query(
                dq_log_insert_sql(chunk | length), bindings=bindings, abridge_sql_log=True) %}
        {% endfor %}
    {% endif %}
{% endmacro %}
