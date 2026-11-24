-- Each pump event against its own ordinary hours: one row per (event, hour offset, metric)
-- with a value. control_median and the shares come from the event's control hours of the
-- metric's pool (macro pump_metrics) at the same offset; excess = value - control_median.
-- The per-event numbers behind mart_pump_profile, kept so that an analysis outside the
-- warehouse can resample events (confidence intervals) without the 560k control rows.

with event_values as (

    {{ pump_metric_values(ref('fct_pump_event_window')) }}

),

control_values as (

    {{ pump_metric_values(ref('fct_pump_control_window'), control=true) }}

)

select
    event_values.pump_event_id,
    event_values.offset_hours,
    event_values.metric,
    event_values.value,
    count(control_values.value) as control_hours,
    median(control_values.value) as control_median,
    count_if(control_values.value > 0) / nullif(count(control_values.value), 0)
        as control_share_above_zero,
    count_if(control_values.value < 0) / nullif(count(control_values.value), 0)
        as control_share_below_zero,
    event_values.value - median(control_values.value) as excess
from event_values
left join control_values
    on
        event_values.pump_event_id = control_values.pump_event_id
        and event_values.offset_hours = control_values.offset_hours
        and event_values.metric = control_values.metric
group by
    event_values.pump_event_id,
    event_values.offset_hours,
    event_values.metric,
    event_values.value
