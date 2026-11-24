-- The pump profile against ordinary hours: one row per (event scope, hour offset, metric).
-- Event side: medians and quartiles over events with a value at that offset. Control side:
-- the control hours of those same events only (matched by symbol and by data coverage).
-- median_excess: the median over events of (event value - median of its own controls),
-- the number the findings quote; the per-event values are fct_pump_event_excess.
-- Scope: all events, or the event's own event_scope from fct_pump_events: single-coin
-- (market_breadth = 1) or multi-coin (2+ symbols past the threshold in the same hour,
-- a market move rather than a coin's own pump).
-- Year metrics compare with the year pool, futures metrics (~30 days) with the recent pool
-- (macro pump_metrics).

with per_event as (

    select
        pump_event_id,
        offset_hours,
        metric,
        value,
        excess
    from {{ ref('fct_pump_event_excess') }}

),

control_values as (

    {{ pump_metric_values(ref('fct_pump_control_window'), control=true) }}

),

matched_controls as (

    -- Only the controls of events that have a value there: an event outside the futures
    -- history must not bring its controls into the comparison.
    select control_values.*
    from control_values
    inner join per_event
        on
            control_values.pump_event_id = per_event.pump_event_id
            and control_values.offset_hours = per_event.offset_hours
            and control_values.metric = per_event.metric

),

scopes as (

    -- Every event twice: in 'all' and in its own scope.
    select
        pump_event_id,
        'all' as event_scope
    from {{ ref('fct_pump_events') }}
    union all
    select
        pump_event_id,
        event_scope
    from {{ ref('fct_pump_events') }}

),

event_stats as (

    select
        scopes.event_scope,
        per_event.offset_hours,
        per_event.metric,
        count(*) as events,
        median(per_event.value) as event_median,
        percentile_cont(0.25) within group (order by per_event.value) as event_p25,
        percentile_cont(0.75) within group (order by per_event.value) as event_p75,
        median(per_event.excess) as median_excess
    from per_event
    inner join scopes
        on per_event.pump_event_id = scopes.pump_event_id
    group by scopes.event_scope, per_event.offset_hours, per_event.metric

),

control_stats as (

    select
        scopes.event_scope,
        matched_controls.offset_hours,
        matched_controls.metric,
        count(*) as control_hours,
        median(matched_controls.value) as control_median
    from matched_controls
    inner join scopes
        on matched_controls.pump_event_id = scopes.pump_event_id
    group by scopes.event_scope, matched_controls.offset_hours, matched_controls.metric

)

select
    event_stats.event_scope,
    event_stats.offset_hours,
    event_stats.metric,
    event_stats.events,
    event_stats.event_median,
    event_stats.event_p25,
    event_stats.event_p75,
    coalesce(control_stats.control_hours, 0) as control_hours,
    control_stats.control_median,
    event_stats.median_excess
from event_stats
left join control_stats
    on
        event_stats.event_scope = control_stats.event_scope
        and event_stats.offset_hours = control_stats.offset_hours
        and event_stats.metric = control_stats.metric
