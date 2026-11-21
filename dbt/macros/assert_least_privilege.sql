{#
  Smoke check of the dbt service user, the twin of ingestion/check_snowflake.py: fail
  unless the session runs as `role` and can use no role besides `allowed`. A role granted
  to CRYPTO_DBT by mistake (or to PUBLIC, as the trial did) would widen what dbt can do.
  Context functions only: the warehouse is not resumed.

    dbt run-operation assert_least_privilege
#}
{% macro assert_least_privilege(role="CRYPTO_TRANSFORMER", allowed=["CRYPTO_TRANSFORMER", "PUBLIC"]) %}
    {% set sql %}
        select
            current_user() as user_name,
            current_role() as role_name,
            current_secondary_roles() as secondary_roles,
            current_available_roles() as available_roles
    {% endset %}
    {# by position: the column order is fixed by the select above #}
    {% set row = run_query(sql).rows[0] %}
    {% set available = fromjson(row[3]) %}
    {{ log("user " ~ row[0] ~ ", role " ~ row[1]
           ~ ", secondary " ~ row[2], info=true) }}
    {{ log("available roles: " ~ available | join(", "), info=true) }}
    {% if row[1] != role %}
        {{ exceptions.raise_compiler_error("dbt runs as " ~ row[1] ~ ", expected " ~ role) }}
    {% endif %}
    {% set unexpected = available | reject("in", allowed) | list %}
    {% if unexpected %}
        {{ exceptions.raise_compiler_error("Roles beyond " ~ allowed | join(", ") ~ ": " ~ unexpected | join(", ")) }}
    {% endif %}
    {{ log("OK: dbt connected with its role only", info=true) }}
{% endmacro %}
