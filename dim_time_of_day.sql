use database {{ Database }};
use schema {{ schema }};

-- Dim Time of Day

CREATE OR REPLACE TABLE DIM_TIME_OF_DAY AS
    SELECT
        TIME_PK TIME_OF_DAY_KEY
        , HOUR_24 TIME_OF_DAY_HH24
        , HOUR_12 TIME_OF_DAY_HH12
        -- , HOUR_1 HOUR_OF_DAY
        , FLOOR(TIME_PK/60) MINUTE_OF_DAY
        , INTERVAL_05 INTERVAL_05_MIN
        , INTERVAL_15 INTERVAL_15_MIN
        , INTERVAL_30 INTERVAL_30_MIN
        , INTERVAL_1HR INTERVAL_01_HOUR
        , SERVING_TIME
        , LAST_UPDATE_DATE
FROM (
      select
            to_date('00:00:00','HH24:MI:SS') as TEMP_TIMESTAMP,
            seq8() as TEMP_SEQUENCE,
            row_number() over (order by TEMP_SEQUENCE) as TIME_PK,
            -- to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') HOUR_24, --original
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS') HOUR_24,
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH12:MI:SS PM') HOUR_12,
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24') as HOUR_1,
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'MI') as MINUTE_1,
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'SS') as SECOND_1,
            case
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') <
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'05:00','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':00:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':04:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'05:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'09:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':05:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':09:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'10:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'14:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':10:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':14:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'15:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'19:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':15:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':19:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'20:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'24:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':20:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':24:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'25:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'29:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':25:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':29:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'30:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'34:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':30:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':34:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'35:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'39:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':35:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':39:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'40:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'44:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':40:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':44:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'45:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'49:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':45:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':49:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'50:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'54:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':50:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':54:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'55:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'59:59','HH24:MI:SS')	then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':55:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':59:59'
            end INTERVAL_05,
            case
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') <
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'15:00','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':00:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':14:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'15:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'29:59','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':15:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':29:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'30:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'44:59','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':30:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':44:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'45:00','HH24:MI:SS') and
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'59:59','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':45:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':59:59'
            end INTERVAL_15,
            case
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') <
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'30:00','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':00:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':29:59'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') >
                to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:')||'29:59','HH24:MI:SS') then
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':30:00-'||
                to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':59:59'

            end INTERVAL_30,
            to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':00:00-'|| to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24')||':59:59' as INTERVAL_1HR,
            case
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between to_time('09:00:00','HH24:MI:SS') and to_time('10:59:59','HH24:MI:SS') then 'BREAKFAST'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between to_time('11:00:00','HH24:MI:SS') and to_time('14:59:59','HH24:MI:SS')  then 'LUNCH'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between to_time('15:00:00','HH24:MI:SS') and to_time('16:59:59','HH24:MI:SS')  then 'AFTERNOON'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between to_time('17:00:00','HH24:MI:SS') and to_time('20:59:59','HH24:MI:SS')  then 'DINNER'
                when to_time(to_char(dateadd(second,TEMP_SEQUENCE,TEMP_TIMESTAMP),'HH24:MI:SS'),'HH24:MI:SS') between to_time('21:00:00','HH24:MI:SS') and to_time('22:59:59','HH24:MI:SS')  then 'LATE NIGHT'
                else 'OTHER' end SERVING_TIME,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS LAST_UPDATE_DATE

        from table(generator(rowcount => 86400))
    ) TEMP_QUERY;