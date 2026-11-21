-- Every pump event has exactly 2 * pump_window_hours + 1 rows in the window mart, one per
-- offset: the profile averages by offset assume it. Returns the events that differ.

select
    events.pump_event_id,
    count(window_rows.pump_event_id) as window_row_count
from {{ ref('fct_pump_events') }} as events
left join {{ ref('fct_pump_event_window') }} as window_rows
    on events.pump_event_id = window_rows.pump_event_id
group by events.pump_event_id
having count(window_rows.pump_event_id) <> {{ 2 * var('pump_window_hours') + 1 }}
