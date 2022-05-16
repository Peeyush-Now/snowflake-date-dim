use database {{ Database }};
use schema {{ schema }};

create or replace view date as
select
    DATE_KEY                   as "Date Key"
    , CALENDAR_DATE            as "Calendar Date"
    , DATE_DESC                as "Date Desc"
    , DAY_NAME                 as "Day Name"
    , DAY_NAME_ABRV            as "Day Name Abrv"
    , DAY_IN_WEEK_NO           as "Day In Week No"
    , DAY_IN_MONTH             as "Day In Month"
    , DAY_STATUS               as "Day Status"
    , WEEKDAY_FLAG             as "Weekday Flag"
    , WEEKEND_FLAG             as "Weekend Flag"
    , WEEK_COMMENCING_DATE_KEY as "Week Commencing Date Key"
    , WEEK_COMMENCING_DATE     as "Week Commencing Date"
    , WEEK_ENDING_DATE_KEY     as "Week Ending Date Key"
    , WEEK_ENDING_DATE         as "Week Ending Date"
    , CAL_YEAR                 as "Calendar Year"
    , MONTH_NO                 as "Calendar Month No"
    , MONTH                    as "Calendar Month"
    , MONTH_NAME               as "Calendar Month Name"
    , MONTH_NAME_ABRV          as "Calendar Month Name Abrv"
    , MONTHEND_FLAG            as "Calendar Monthend Flag"
    , CAL_WEEK_IN_YEAR         as "Calendar Week In Year"
    , CAL_DAY_IN_YEAR          as "Calendar Day In Year"
    , CAL_QUARTER_NO           as "Calendar Quarter No"
    , CAL_QUARTER              as "Calendar Quarter"
    , FIN_YEAR                 as "Financial Year"
    , FIN_YEAR_NAME            as "Financial Year Name"
    , FIN_YEAR_STATUS          as "Financial Year Status"
    , FIN_YEARHALF             as "Financial Yearhalf"
    , FIN_PERIOD               as "Financial Period"
    , FIN_PERIOD_STATUS        as "Financial Period Status"
    , FIN_QUARTER              as "Financial Quarter"
    , FIN_QUARTER_NO           as "Financial Quarter No"
    , FIN_QUARTER_STATUS       as "Financial Quarter Status"
    , FIN_WEEK_IN_YEAR         as "Financial Week In Year"
    , FIN_WEEK_STATUS          as "Financial Week Status"
    , FIN_DAY_IN_YEAR          as "Financial Day In Year"
    , CURRENT_CAL_WTD          as "Current Cal WTD"
    , CURRENT_CAL_MTD          as "Current Cal MTD"
    , CURRENT_CAL_PTD          as "Current Cal PTD"
    , CURRENT_CAL_QTD          as "Current Cal QTD"
    , CURRENT_CAL_YTD          as "Current Cal YTD"
    , PREVIOUS_WK_CAL_WTD      as "Previous Wk Cal WTD"
    , PREVIOUS_YR_CAL_WTD      as "Previous Yr Cal WTD"
    , PREVIOUS_YR_CAL_PTD      as "Previous Yr Cal PTD"
    , PREVIOUS_YR_CAL_QTD      as "Previous Yr Cal QTD"
    , PREVIOUS_YR_CAL_YTD      as "Previous Yr Cal YTD"
    , case 
        when CURRENT_CAL_WTD = 'YES' then 'WTD'
        when PREVIOUS_WK_CAL_WTD = 'YES' then 'WTD-LW'
        WHEN PREVIOUS_YR_CAL_WTD = 'YES' then 'WTD-LY'
        end "Financial Week WTD"
from
    dim_date;