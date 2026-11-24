-- One row per (pump event, hour offset) from -pump_window_hours to +pump_window_hours:
-- the hourly profile around each event, the shape the dashboard averages by offset.
-- Offset 0 is the event hour. Values relative to the event start (end of hour -1).
-- Every event gets every offset; hours outside the loaded history are NULL, not missing,
-- so "rows = events x offsets" holds and averages see the gaps.
-- Measured by the macro pump_window, like the ordinary hours of fct_pump_control_window.

with anchors as (

    select
        pump_event_id,
        symbol,
        event_open_time_ms as anchor_open_time_ms
    from {{ ref('fct_pump_events') }}

)

{{ pump_window('anchors') }}
