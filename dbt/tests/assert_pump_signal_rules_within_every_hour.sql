-- Every rule of mart_pump_signal_check filters the hours of every_hour: per day it cannot
-- have more hours or more pumps, and it has no day every_hour lacks. Returns the rows
-- that break it.

with every_hour as (

    select
        signal_date,
        signal_hours,
        followed_by_pump
    from {{ ref('mart_pump_signal_check') }}
    where rule = 'every_hour'

)

select
    rules.rule,
    rules.signal_date,
    rules.signal_hours,
    rules.followed_by_pump,
    every_hour.signal_hours as every_hour_signal_hours,
    every_hour.followed_by_pump as every_hour_followed_by_pump
from {{ ref('mart_pump_signal_check') }} as rules
left join every_hour
    on rules.signal_date = every_hour.signal_date
where
    every_hour.signal_date is null
    or rules.signal_hours > every_hour.signal_hours
    or rules.followed_by_pump > every_hour.followed_by_pump
