{#
  Build models in the schema their config names (STAGING, MARTS) as is, instead of dbt's
  default "<target schema>_<custom schema>" (STAGING_STAGING). The project has one
  environment, written by one service user; a team would give each developer a schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
