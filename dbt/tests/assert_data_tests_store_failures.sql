-- Every data test of the project keeps its failing rows in DQ: a warn test as a table
-- (the findings the DQ report reads), a stop test at least as a view. Built from the
-- project graph at compile time and reads no table: a test that stops storing its
-- findings (store_failures_as: ephemeral, or the project default removed) fails here.

{% set violations = [] %}
{% if execute %}
    {% for node in graph.nodes.values()
        if node.resource_type == 'test' and node.package_name == project_name %}
        {% set severity = node.config.severity | lower %}
        {% set stored_as = node.config.store_failures_as %}
        {% if stored_as not in ['table', 'view'] or (severity == 'warn' and stored_as != 'table') %}
            {% do violations.append([node.unique_id, severity, stored_as | string]) %}
        {% endif %}
    {% endfor %}
{% endif %}

{% for test_unique_id, severity, stored_as in violations %}
select
    '{{ test_unique_id }}' as test_unique_id,
    '{{ severity }}' as severity,
    '{{ stored_as }}' as store_failures_as
{% if not loop.last %}union all{% endif %}
{% else %}
select
    null::varchar as test_unique_id,
    null::varchar as severity,
    null::varchar as store_failures_as
where false
{% endfor %}
