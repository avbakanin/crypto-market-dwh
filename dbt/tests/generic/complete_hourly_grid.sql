{#
  Every hour from a series' first point to the last point that should exist as of the
  latest extract batch, per partition (symbol / contract). Returns the missing hours.

  Catches what LAG-based gap checks and source freshness cannot: a hole inside a series
  and a series that stopped early (its tail is missing, no neighbour to compare with).

  column_name          the point time in Unix ms (integer arithmetic, no time zones)
  partition_by         the series key
  ready_after_minutes  when a point exists after its hour starts: 60 for a candle (closed
                       an hour after it opens), 5 for open interest (publication lag)
  max_hours            calendar size; a longer series returns a row saying so, never
                       a silent partial check
#}
{% test complete_hourly_grid(model, column_name, partition_by, ready_after_minutes, max_hours=20000) %}

with points as (

    select {{ partition_by }} as series, {{ column_name }} as point_ms, _batch_id
    from {{ model }}

),

as_of as (

    -- _batch_id is the extract's UTC as-of moment: 20261121T120734Z.
    select date_part(
        epoch_millisecond, to_timestamp_ntz(max(_batch_id), 'YYYYMMDD"T"HH24MISS"Z"')
    ) as as_of_ms
    from points

),

bounds as (

    select
        points.series,
        min(points.point_ms) as first_ms,
        floor((any_value(as_of.as_of_ms) - {{ ready_after_minutes }} * 60000) / 3600000)
        * 3600000 as last_ms
    from points
    cross join as_of
    group by points.series

),

calendar as (

    -- row_number, not seq4(): seq4() may have gaps.
    select row_number() over (order by seq4()) - 1 as n
    from table(generator(rowcount => {{ max_hours }}))

),

expected as (

    select bounds.series, bounds.first_ms + calendar.n * 3600000 as point_ms
    from bounds
    inner join calendar
        on bounds.first_ms + calendar.n * 3600000 <= bounds.last_ms

)

select
    expected.series,
    to_timestamp_ntz(expected.point_ms, 3) as missing_hour_utc,
    'missing' as issue
from expected
left join points
    on expected.series = points.series and expected.point_ms = points.point_ms
where points.point_ms is null

union all

select
    series,
    to_timestamp_ntz(first_ms, 3),
    'series longer than the calendar of {{ max_hours }} hours: not fully checked'
from bounds
where (last_ms - first_ms) / 3600000 >= {{ max_hours }}

{% endtest %}
