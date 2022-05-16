use database {{ Database }};
use schema {{ schema }};

create or replace view time_of_day as
select
    time_of_day_key    as "Time Of Day Key"
    , time_of_day_hh24 as "Time Of Day HH24"
    , time_of_day_hh12 as "Time Of Day HH12"
    , minute_of_day    as "Minute Of Day"
    , interval_01_hour as "Interval 01 Hour"
    , interval_30_min  as "Interval 30 Min"
    , interval_15_min  as "Interval 15 Min"
    , interval_05_min  as "Interval 05 Min"
    , serving_time     as "Day-Part"
from
    dim_time_of_day;