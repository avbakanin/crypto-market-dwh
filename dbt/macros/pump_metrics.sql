{#-
    The metrics of the pump profile and the control pool each one is compared with:
    year metrics (price, volume, taker share, funding: a year of history) with the year
    pool, futures metrics (open interest, long/short: ~30 days) with the recent pool.
    One list for fct_pump_event_excess and mart_pump_profile.
-#}
{% macro pump_metrics() %}
{{ return([
    ('return_since_start', 'year'),
    ('return_since_anchor_close', 'year'),
    ('volume_ratio', 'year'),
    ('taker_buy_share', 'year'),
    ('funding_premium_8h', 'year'),
    ('open_interest_change_since_start', 'recent'),
    ('top_account_ratio_change_since_start', 'recent'),
    ('top_position_ratio_change_since_start', 'recent'),
    ('global_account_ratio_change_since_start', 'recent'),
]) }}
{% endmacro %}


{#-
    One row per (pump event, offset, metric) with a value: the long form of a window built
    by pump_window. For the control window (control=true) only the hours of the metric's
    own pool.
-#}
{% macro pump_metric_values(relation, control=false) %}
{%- for metric, pool in pump_metrics() %}
    select
        pump_event_id,
        offset_hours,
        '{{ metric }}' as metric,
        {{ metric }} as value
    from {{ relation }}
    where {% if control %}is_{{ pool }}_control and {% endif %}{{ metric }} is not null
    {%- if not loop.last %}
    union all
    {%- endif %}
{%- endfor %}
{% endmacro %}
