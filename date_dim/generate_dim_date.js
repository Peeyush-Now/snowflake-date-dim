use database {{ Database }};
use schema {{ schema }};

-- Note: The logic for derivation of various columns are maintained in the roll_date_dim()
-- The logic here may not be up to Date (pun intended)!

CREATE OR REPLACE PROCEDURE transform.GENERATE_DIM_DATE(START_DATE STRING, END_DATE STRING, INITIAL_LOAD BOOLEAN)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    COMMENT = 'Called with fully qualified presentation & stage table name and flag to perform full or incremental processing'
    AS
    $$
        // JavaScript code is case sensitive. Input parameter names (not values) are converted to Upper Case unless inside double quotes.
        // Safer to use all variables and parameters in upper case.
        
        const INGEST_DB       = 'PROD_INGEST_DB';         // for logging
        const INGEST_SCHEMA   = 'RAW_BIZ_FILES';               // for logging
        const MODEL_SCHEMA    = 'PRESENTATION';               // for logging
        
        const STAGE_SCHEMA    = 'TRANSFORM';                  // for logging
        var STAGE_TABLE     = 'STAGE_DATE_10';                // for logging
        const PROCEDURE_NAME  = 'GENERATE_' + STAGE_TABLE;     // for logging
        
        // this is an embedded javascript function, CAN NOT be moved out of procedure.
        // this allows cleaner invocation of stored proc API, with the sql provided
        // the sql is expected to return 1 row at max and is logged in the audit table.
        
        function execute_and_return (sql) {   
                var result_sql = snowflake.createStatement({sqlText: sql}).execute();
                result_sql.next();
                return result_sql.getColumnValue(1);       
        }
        
        function execute_and_log (sql) {   
                var result_sql = snowflake.createStatement({sqlText: sql}).execute();
                result_sql.next();
                var SQL_STEP = result_sql.getQueryId();
                var STEP_RESULT = result_sql.getColumnName(1) + ': ' + result_sql.getColumnValue(1);
                
                snowflake.execute ({sqlText: `INSERT INTO ${MODEL_SCHEMA}.AUDIT_TABLE (PROCEDURE_NAME, SQL_STEP, STEP_RESULT, AUDIT_TIMESTAMP) VALUES (?,?,?,?)`
                                        , binds: [PROCEDURE_NAME, SQL_STEP, STEP_RESULT, Date.now().toString()] });
                // for debugging only
                // return result_sql.getColumnName(1) + ': ' + result_sql.getColumnValue(1);       
        }        
        
        function log_error(err){
        snowflake.execute ({sqlText: `INSERT INTO ${MODEL_SCHEMA}.ERROR_TABLE (PROCEDURE_NAME, ERROR_CODE, ERROR_MESSAGE, ERROR_STATE, STACK_TRACE
                ,ERROR_TIMESTAMP) VALUES (?,?,?,?,?,?)`, binds: [PROCEDURE_NAME, err.code, err.state, err.message, err.stackTraceTxt, Date.now().toString()] });
        }
        
        
        try {   
                // 1. determine the span of the date dimension to be generated.
                // -----------------------------
                
                var sql_date_range = `SELECT DATEDIFF(DAY,TO_DATE('${START_DATE}', 'YYYY-MM-DD'), TO_DATE('${END_DATE}', 'YYYY-MM-DD'))`;
                var row_count = execute_and_return(sql_date_range);
                
                // truncate doesnot need a warehouse, delete does. Performance is the same.
                var sql_truncate_stage_date_10 = `TRUNCATE TABLE ${STAGE_SCHEMA}.STAGE_DATE_10`;
                execute_and_log (sql_truncate_stage_date_10);
                
                var sql_generate_dates = `INSERT INTO ${STAGE_SCHEMA}.STAGE_DATE_10 (CALENDAR_DATE, LAST_UPDATE_DATE )
                                            SELECT 
                                                dateadd(day,row_number() over (order by seq4()) - 1, TO_DATE('${START_DATE}', 'YYYY-MM-DD'))
                                                , CURRENT_TIMESTAMP
                                            from table(generator(rowcount => ${row_count}))
                                            `;
                
                execute_and_log (sql_generate_dates);
               
               // 2a. compute date derived columns 
               //--------------------------------------------------------- 
                var sql_truncate_stage_date_20A = `TRUNCATE TABLE ${STAGE_SCHEMA}.STAGE_DATE_20A`;
                execute_and_log (sql_truncate_stage_date_20A);
                
                var sql_derive_cal_dates = `INSERT INTO ${STAGE_SCHEMA}.STAGE_DATE_20A
                                                SELECT 
                                                       CALENDAR_DATE                                                                calendar_date
                                                        , NULL                                                                      date_desc
                                                        , case dayname(CALENDAR_DATE)
                                                            when 'Sun' then 'Sunday'
                                                            when 'Mon' then 'Monday'
                                                            when 'Tue' then 'Tuesday'
                                                            when 'Wed' then 'Wednesday'
                                                            when 'Thu'  then 'Thursday'
                                                            when 'Fri' then 'Friday'
                                                            when 'Sat' then 'Saturday'
                                                          else 'Invalid Day' end                                                    day_name
                                                        , dayname(CALENDAR_DATE)                                                    day_name_abrv 
                                                        , date_part('dayofweek', CALENDAR_DATE)                                     day_in_week_no 
                                                        , day(CALENDAR_DATE)                                                        day_in_month 
                                                        , case 
                                                            when CALENDAR_DATE = CURRENT_DATE then 'Today'
                                                            when CALENDAR_DATE = CURRENT_DATE - 1  then 'Previous Day'
                                                            when (mod(year(CALENDAR_DATE) - 1, 4) = 0 and CALENDAR_DATE = current_date -365)
                                                                   OR (mod(year(CALENDAR_DATE) - 1, 4) <> 0 and CALENDAR_DATE = current_date -364) 
                                                                    then 'Previous Day Last Year'
                                                            else 'Other' end                                                           day_status
                                                        , case when dayname(CALENDAR_DATE) not in ('Sun', 'Sat') 
                                                            then 1 else 0 end                                                       weekday_flag 
                                                        , case when dayname(CALENDAR_DATE)  in ('Sun', 'Sat') 
                                                            then 1 else 0 end                                                       weekend_flag 
                                                        , NULL                                                                      week_commencing_date_key
                                                        , NULL                                                                      week_commencing_date
                                                        , NULL                                                                      week_ending_date_key
                                                        , NULL                                                                      week_ending_date
                                                        , month(CALENDAR_DATE)                                                      month_no 
                                                        , to_char(CALENDAR_DATE, 'yyyy-mm')                                         month 
                                                        , to_char(CALENDAR_DATE, 'MMMM')                                            month_name 
                                                        , to_char(CALENDAR_DATE, 'MON')                                             month_name_abrv 
                                                        , NULL                                                                      monthend_flag
                                                        , date_part('dayofyear', CALENDAR_DATE)                                     cal_day_in_year 
                                                        , week(CALENDAR_DATE)                                                       cal_week_in_year 
                                                        , QUARTER(CALENDAR_DATE)                                                    cal_quarter_no 
                                                        , year(CALENDAR_DATE)::varchar || '0'|| quarter(CALENDAR_DATE)::varchar     cal_quarter 
                                                        , year(CALENDAR_DATE)                                                       cal_year 
                                                        , NULL                                                                      current_cal_wtd 
                                                        , NULL                                                                      current_cal_mtd 
                                                        , NULL                                                                      current_cal_ptd 
                                                        , NULL                                                                      current_cal_qtd 
                                                        , NULL                                                                      current_cal_ytd 
                                                        , NULL                                                                      previous_wk_cal_wtd 
                                                        , NULL                                                                      previous_yr_cal_wtd 
                                                        , NULL                                                                      previous_yr_cal_ptd 
                                                        , NULL                                                                      previous_yr_cal_qtd 
                                                        , NULL                                                                      previous_yr_cal_ytd 
                                                        , CURRENT_TIMESTAMP                                                         LAST_UPDATE_DATE
                                                    FROM
                                                        ${STAGE_SCHEMA}.STAGE_DATE_10`;
                execute_and_log (sql_derive_cal_dates);
               
               // 2b. Derive financial date components 
               //---------------------------------------------
               sql_truncate_stage_date_20B = `TRUNCATE TABLE ${STAGE_SCHEMA}.STAGE_DATE_20B`;
                execute_and_log (sql_truncate_stage_date_20B);
                
                var sql_derive_fin_dates = `INSERT INTO ${STAGE_SCHEMA}.STAGE_DATE_20B
                                                SELECT 
                                                    calendar_date                                                       calendar_date
                                                    , week_commencing                                                   week_commencing
                                                    , NULL                                                              fin_day_in_year 
                                                    , financial_week                                                    fin_week_in_year
                                                    , to_Number(substring(financial_week, 6, 2), '99')                  fin_week 
                                                    , financial_period                                                  fin_period 
                                                    , financial_quarter                                                 fin_quarter_no 
                                                    , financial_year||'-0'||financial_quarter                           fin_quarter 
                                                    , case when financial_quarter in (1,2) then 1 else 2 end            fin_yearhalf
                                                    , financial_year                                                    fin_year
                                                    , financial_year_format                                             fin_year_name
                                                    , NULL                                                              fin_year_Status
                                                    , NULL                                                              fin_quarter_Status
                                                    , NULL                                                              fin_period_Status
                                                    , NULL                                                              fin_week_Status
                                                    , CURRENT_TIMESTAMP                                                 LAST_UPDATE_DATE
                                                 FROM 
                                                    ${INGEST_DB}.${INGEST_SCHEMA}.TBL_FinancialDates`;
                                                    
               execute_and_log (sql_derive_fin_dates);
               
               // 3. Join data and derive some more columns 
               //--------------------------------------------------------
                var sql_truncate_stage_date_30 = `TRUNCATE TABLE ${STAGE_SCHEMA}.STAGE_DATE_30`;
                execute_and_log (sql_truncate_stage_date_30);
                
                var sql_get_curr_fin_year = `SELECT 
                                                    fin_year ::number
                                                    , fin_quarter_no
                                                    , substr(fin_period, 8,2) ::number
                                                    , fin_week
                                                from ${STAGE_SCHEMA}.STAGE_DATE_20B 
                                                where calendar_date = CURRENT_DATE
                                            `;
                var result_sql = snowflake.createStatement({sqlText: sql_get_curr_fin_year}).execute();
                result_sql.next();
                
                var curr_fin_year       = result_sql.getColumnValue(1);
                var curr_fin_quarter    = result_sql.getColumnValue(2);
                var curr_fin_period     = result_sql.getColumnValue(3);
                var curr_fin_week       = result_sql.getColumnValue(4);

                var sql_derive_previous_dates = `INSERT INTO ${STAGE_SCHEMA}.STAGE_DATE_30             
                                                    SELECT 
                                                        STAGE_DATE_20A.calendar_date                                                                                                       calendar_date 
                                                        , day_name||', '||to_char(STAGE_DATE_20A.calendar_date, 'MMMM dd,YYYY')                                                            date_desc
                                                        , day_name                                                                                                                         day_name
                                                        , day_name_abrv                                                                                                                    day_name_abrv 
                                                        , day_in_week_no                                                                                                                   day_in_week_no 
                                                        , day_in_month                                                                                                                     day_in_month 
                                                        , day_status                                                                                                                       day_status
                                                        , weekday_flag                                                                                                                     weekday_flag 
                                                        , weekend_flag                                                                                                                     weekend_flag 
                                                        , to_char(last_day(STAGE_DATE_20A.calendar_date, 'week') - 6, 'YYYYMMDD')                                                          week_commencing_date_key
                                                        , last_day(STAGE_DATE_20A.calendar_date, 'week') - 6                                                                               week_commencing_date
                                                        , to_char(last_day(STAGE_DATE_20A.calendar_date, 'week'), 'YYYYMMDD')                                                              week_ending_date_key
                                                        , last_day(STAGE_DATE_20A.calendar_date, 'week')                                                                                   week_ending_date
                                                        , month_no                                                                                                                         month_no 
                                                        , month                                                                                                                            month 
                                                        , month_name                                                                                                                       month_name 
                                                        , month_name_abrv                                                                                                                  month_name_abrv 
                                                        , case when STAGE_DATE_20A.calendar_date = last_day(STAGE_DATE_20A.calendar_date, 'month') then 'Y' else 'N' end                   monthend_flag
                                                        , cal_day_in_year                                                                                                                  cal_day_in_year 
                                                        , cal_week_in_year                                                                                                                 cal_week_in_year 
                                                        , cal_quarter_no                                                                                                                   cal_quarter_no 
                                                        , cal_quarter                                                                                                                      cal_quarter 
                                                        , cal_year                                                                                                                         cal_year
                                                        , week_commencing                                                                                                                  week_commencing 
                                                        , (fin_week - 1) * 7 + 
                                                            case when date_part('dayofweek', STAGE_DATE_20A.calendar_date) = 0 then 7 
                                                                else date_part('dayofweek', STAGE_DATE_20A.calendar_date) end                                                              fin_day_in_year 
                                                        , fin_week_in_year                                                                                                                 fin_week_in_year
                                                        , fin_week                                                                                                                         fin_week 
                                                        , fin_period                                                                                                                       fin_period 
                                                        , fin_quarter_no                                                                                                                   fin_quarter_no 
                                                        , fin_quarter                                                                                                                      fin_quarter 
                                                        , fin_yearhalf                                                                                                                     fin_yearhalf
                                                        , fin_year                                                                                                                         fin_year
                                                        , fin_year_name                                                                                                                    fin_year_name 
                                                        , case when fin_year = ${curr_fin_year} then 'Current Year'
                                                                    when fin_year = ${curr_fin_year} -1 then 'Previous Year'
                                                                    when fin_year = ${curr_fin_year} -2 then '2 Year Back'
                                                                    else NULL end                                                                                                          fin_year_status
                                                        , case when fin_year = ${curr_fin_year} and fin_quarter_no = ${curr_fin_quarter} then 'Current Quarter'
                                                                when fin_year = ${curr_fin_year} and fin_quarter_no = ${curr_fin_quarter} -1 then 'Previous Quarter'
                                                                when fin_year = ${curr_fin_year} -1 and fin_quarter_no = ${curr_fin_quarter} then 'Current Quarter Last Year'
                                                                else NULL end                                                                                                              fin_quarter_status
                                                        , case when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} then 'Current Period'
                                                                when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} -1 then 'Previous Period'
                                                                when fin_year = ${curr_fin_year} -1 and substr(fin_period, 8,2) = ${curr_fin_period} then 'Current Period Last Year'
                                                                else NULL end                                                                                                              fin_period_Status
                                                        , case when fin_year = ${curr_fin_year} and fin_week = ${curr_fin_week} then 'Current Week'
                                                                when fin_year = ${curr_fin_year} and fin_week = ${curr_fin_week} -1 then 'Previous Week'
                                                                when fin_year = ${curr_fin_year} -1 and fin_week = ${curr_fin_week} then 'Current Week Last Year'
                                                                when fin_year = ${curr_fin_year} -1 and fin_week = ${curr_fin_week} -1 then 'Previous Week Last Year'    
                                                                else NULL end                                                                                                               fin_week_status
                                                        , case when last_day(current_date, 'week') - 6 <= STAGE_DATE_20A.calendar_date
                                                                        and STAGE_DATE_20A.calendar_date <= Current_date then 'YES' else 'NO' end                                          current_cal_wtd 
                                                        , case when month = to_char(current_date, 'YYYY-MM') 
                                                            and STAGE_DATE_20A.calendar_date <= Current_date then 'YES' else 'NO' end                                                      current_cal_mtd 
                                                        , case when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} 
                                                            and STAGE_DATE_20A.calendar_date <= Current_date then 'YES' else 'NO' end                                                      current_cal_ptd 
                                                        , case when cal_quarter = year(current_date)::varchar || '0'|| quarter(current_date)::varchar 
                                                            and STAGE_DATE_20A.calendar_date <= Current_date then 'YES' else 'NO' end                                                      current_cal_qtd 
                                                        , case when cal_year = year(current_date) 
                                                            and STAGE_DATE_20A.calendar_date <= Current_date then 'YES' else 'NO' end                                                      current_cal_ytd 
                                                        , case when last_day(current_date, 'week') - 13 <= STAGE_DATE_20A.calendar_date
                                                                        and STAGE_DATE_20A.calendar_date <= Current_date -7 then 'YES' else 'NO' end                                       previous_wk_cal_wtd 
                                                        , case when 
                                                            last_day((case when mod(year(STAGE_DATE_20A.calendar_date) - 1, 4) = 0 then current_date -366 else current_date - 365 end), 'week') - 6
                                                                <= STAGE_DATE_20A.calendar_date and to_char(STAGE_DATE_20A.calendar_date,'mmdd') <= to_char(Current_date, 'mmdd') 
                                                                then 'YES' else 'NO' end                                                                                                    previous_yr_cal_wtd 
                                                        , case when fin_year = ${curr_fin_year} -1 and substr(fin_period, 8,2) = ${curr_fin_period}
                                                            and to_char(STAGE_DATE_20A.calendar_date,'mmdd') <= to_char(Current_date, 'mmdd') then 'YES' else 'NO' end                      previous_yr_cal_ptd 
                                                        , case when cal_quarter = (year(current_date) -1) ::varchar || '0'|| quarter(current_date)::varchar 
                                                            and to_char(STAGE_DATE_20A.calendar_date,'mmdd') <= to_char(Current_date, 'mmdd') then 'YES' else 'NO' end                      previous_yr_cal_qtd 
                                                        , case when cal_year = year(current_date) - 1 
                                                            and to_char(STAGE_DATE_20A.calendar_date,'mmdd') <= to_char(Current_date, 'mmdd') then 'YES' else 'NO' end                      previous_yr_cal_ytd 
                                                        , CURRENT_TIMESTAMP                                                                                                                 LAST_UPDATE_DATE
                                                    FROM ${STAGE_SCHEMA}.STAGE_DATE_20A
                                                        LEFT JOIN ${STAGE_SCHEMA}.STAGE_DATE_20B
                                                            ON STAGE_DATE_20A.calendar_date = STAGE_DATE_20B.calendar_date
                                                    `;
                
                execute_and_log (sql_derive_previous_dates);
                
                // 4. Populate the final stage table
                //------------------------------------------------
                var sql_truncate_stage_date = `TRUNCATE TABLE ${STAGE_SCHEMA}.STAGE_DATE`;
                execute_and_log (sql_truncate_stage_date);

                var sql_populate_stage_date = `INSERT INTO ${STAGE_SCHEMA}.STAGE_DATE
                                                     SELECT
                                                          CALENDAR_DATE
                                                         , DATE_DESC
                                                         , DAY_NAME
                                                         , DAY_NAME_ABRV
                                                         , DAY_IN_WEEK_NO
                                                         , DAY_IN_MONTH
                                                         , DAY_STATUS
                                                         , WEEKDAY_FLAG
                                                         , WEEKEND_FLAG
                                                         , WEEK_COMMENCING_DATE_KEY
                                                         , WEEK_COMMENCING_DATE
                                                         , WEEK_ENDING_DATE_KEY
                                                         , WEEK_ENDING_DATE
                                                         , MONTH_NO
                                                         , MONTH
                                                         , MONTH_NAME
                                                         , MONTH_NAME_ABRV
                                                         , MONTHEND_FLAG
                                                         , CAL_DAY_IN_YEAR
                                                         , CAL_WEEK_IN_YEAR
                                                         , CAL_QUARTER_NO
                                                         , CAL_QUARTER
                                                         , CAL_YEAR
                                                         , FIN_DAY_IN_YEAR
                                                         , FIN_WEEK_IN_YEAR
                                                         , FIN_PERIOD
                                                         , FIN_QUARTER_NO
                                                         , FIN_QUARTER
                                                         , FIN_YEARHALF
                                                         , FIN_YEAR
                                                         , FIN_YEAR_NAME
                                                         , FIN_YEAR_STATUS
                                                         , FIN_QUARTER_STATUS
                                                         , FIN_PERIOD_STATUS
                                                         , FIN_WEEK_STATUS
                                                         , CURRENT_CAL_WTD
                                                         , CURRENT_CAL_MTD
                                                         , CURRENT_CAL_PTD
                                                         , CURRENT_CAL_QTD
                                                         , CURRENT_CAL_YTD
                                                         , PREVIOUS_WK_CAL_WTD
                                                         , PREVIOUS_YR_CAL_WTD
                                                         , PREVIOUS_YR_CAL_PTD
                                                         , PREVIOUS_YR_CAL_QTD
                                                         , PREVIOUS_YR_CAL_YTD
                                                         , CURRENT_TIMESTAMP LAST_UPDATE_DATE
                                                        FROM  
                                                            ${STAGE_SCHEMA}.STAGE_DATE_30
                                                      `;
                
                execute_and_log (sql_populate_stage_date);

               // 5. If not Initial Load, Merge in to date dimension
               //-----------------------------------
               if (INITIAL_LOAD == 1) {
                    var sql_truncate_dim_date = `TRUNCATE TABLE ${MODEL_SCHEMA}.DIM_DATE`;
                    execute_and_log (sql_truncate_dim_date);
               }
               
               var sql_populate_dim_date = `MERGE INTO ${MODEL_SCHEMA}.DIM_DATE
                                                USING ${STAGE_SCHEMA}.STAGE_DATE
                                                ON STAGE_DATE.CALENDAR_DATE = DIM_DATE.CALENDAR_DATE
                                                     WHEN MATCHED
                                                          THEN UPDATE
                                                                SET DATE_DESC                     = IFNULL(STAGE_DATE.DATE_DESC                 , DIM_DATE.DATE_DESC)
                                                                    , DAY_NAME                    = IFNULL(STAGE_DATE.DAY_NAME                  , DIM_DATE.DAY_NAME)
                                                                    , DAY_NAME_ABRV               = IFNULL(STAGE_DATE.DAY_NAME_ABRV             , DIM_DATE.DAY_NAME_ABRV)
                                                                    , DAY_IN_WEEK_NO              = IFNULL(STAGE_DATE.DAY_IN_WEEK_NO            , DIM_DATE.DAY_IN_WEEK_NO)
                                                                    , DAY_IN_MONTH                = IFNULL(STAGE_DATE.DAY_IN_MONTH              , DIM_DATE.DAY_IN_MONTH)
                                                                    , DAY_STATUS                  = IFNULL(STAGE_DATE.DAY_STATUS                , DIM_DATE.DAY_STATUS)
                                                                    , WEEKDAY_FLAG                = IFNULL(STAGE_DATE.WEEKDAY_FLAG              , DIM_DATE.WEEKDAY_FLAG)
                                                                    , WEEKEND_FLAG                = IFNULL(STAGE_DATE.WEEKEND_FLAG              , DIM_DATE.WEEKEND_FLAG)
                                                                    , WEEK_COMMENCING_DATE_KEY    = IFNULL(STAGE_DATE.WEEK_COMMENCING_DATE_KEY  , DIM_DATE.WEEK_COMMENCING_DATE_KEY)
                                                                    , WEEK_COMMENCING_DATE        = IFNULL(STAGE_DATE.WEEK_COMMENCING_DATE      , DIM_DATE.WEEK_COMMENCING_DATE)
                                                                    , WEEK_ENDING_DATE_KEY        = IFNULL(STAGE_DATE.WEEK_ENDING_DATE_KEY      , DIM_DATE.WEEK_ENDING_DATE_KEY)
                                                                    , WEEK_ENDING_DATE            = IFNULL(STAGE_DATE.WEEK_ENDING_DATE          , DIM_DATE.WEEK_ENDING_DATE)
                                                                    , MONTH_NO                    = IFNULL(STAGE_DATE.MONTH_NO                  , DIM_DATE.MONTH_NO)
                                                                    , MONTH                       = IFNULL(STAGE_DATE.MONTH                     , DIM_DATE.MONTH)
                                                                    , MONTH_NAME                  = IFNULL(STAGE_DATE.MONTH_NAME                , DIM_DATE.MONTH_NAME)
                                                                    , MONTH_NAME_ABRV             = IFNULL(STAGE_DATE.MONTH_NAME_ABRV           , DIM_DATE.MONTH_NAME_ABRV)
                                                                    , MONTHEND_FLAG               = IFNULL(STAGE_DATE.MONTHEND_FLAG             , DIM_DATE.MONTHEND_FLAG)
                                                                    , CAL_DAY_IN_YEAR             = IFNULL(STAGE_DATE.CAL_DAY_IN_YEAR           , DIM_DATE.CAL_DAY_IN_YEAR)
                                                                    , CAL_WEEK_IN_YEAR            = IFNULL(STAGE_DATE.CAL_WEEK_IN_YEAR          , DIM_DATE.CAL_WEEK_IN_YEAR)
                                                                    , CAL_QUARTER_NO              = IFNULL(STAGE_DATE.CAL_QUARTER_NO            , DIM_DATE.CAL_QUARTER_NO)
                                                                    , CAL_QUARTER                 = IFNULL(STAGE_DATE.CAL_QUARTER               , DIM_DATE.CAL_QUARTER)
                                                                    , CAL_YEAR                    = IFNULL(STAGE_DATE.CAL_YEAR                  , DIM_DATE.CAL_YEAR)
                                                                    , FIN_DAY_IN_YEAR             = IFNULL(STAGE_DATE.FIN_DAY_IN_YEAR           , DIM_DATE.FIN_DAY_IN_YEAR)
                                                                    , FIN_WEEK_IN_YEAR            = IFNULL(STAGE_DATE.FIN_WEEK_IN_YEAR          , DIM_DATE.FIN_WEEK_IN_YEAR)
                                                                    , FIN_PERIOD                  = IFNULL(STAGE_DATE.FIN_PERIOD                , DIM_DATE.FIN_PERIOD)
                                                                    , FIN_QUARTER_NO              = IFNULL(STAGE_DATE.FIN_QUARTER_NO            , DIM_DATE.FIN_QUARTER_NO)
                                                                    , FIN_QUARTER                 = IFNULL(STAGE_DATE.FIN_QUARTER               , DIM_DATE.FIN_QUARTER)
                                                                    , FIN_YEARHALF                = IFNULL(STAGE_DATE.FIN_YEARHALF              , DIM_DATE.FIN_YEARHALF)
                                                                    , FIN_YEAR                    = IFNULL(STAGE_DATE.FIN_YEAR                  , DIM_DATE.FIN_YEAR)
                                                                    , FIN_YEAR_NAME               = IFNULL(STAGE_DATE.FIN_YEAR_NAME             , DIM_DATE.FIN_YEAR_NAME)
                                                                    , FIN_YEAR_STATUS             = IFNULL(STAGE_DATE.FIN_YEAR_STATUS           , DIM_DATE.FIN_YEAR_STATUS)
                                                                    , FIN_QUARTER_STATUS          = IFNULL(STAGE_DATE.FIN_QUARTER_STATUS        , DIM_DATE.FIN_QUARTER_STATUS)
                                                                    , FIN_PERIOD_STATUS           = IFNULL(STAGE_DATE.FIN_PERIOD_STATUS         , DIM_DATE.FIN_PERIOD_STATUS)
                                                                    , FIN_WEEK_STATUS             = IFNULL(STAGE_DATE.FIN_WEEK_STATUS           , DIM_DATE.FIN_WEEK_STATUS)
                                                                    , CURRENT_CAL_WTD             = IFNULL(STAGE_DATE.CURRENT_CAL_WTD           , DIM_DATE.CURRENT_CAL_WTD)
                                                                    , CURRENT_CAL_MTD             = IFNULL(STAGE_DATE.CURRENT_CAL_MTD           , DIM_DATE.CURRENT_CAL_MTD)
                                                                    , CURRENT_CAL_PTD             = IFNULL(STAGE_DATE.CURRENT_CAL_PTD           , DIM_DATE.CURRENT_CAL_PTD)
                                                                    , CURRENT_CAL_QTD             = IFNULL(STAGE_DATE.CURRENT_CAL_QTD           , DIM_DATE.CURRENT_CAL_QTD)
                                                                    , CURRENT_CAL_YTD             = IFNULL(STAGE_DATE.CURRENT_CAL_YTD           , DIM_DATE.CURRENT_CAL_YTD)
                                                                    , PREVIOUS_WK_CAL_WTD         = IFNULL(STAGE_DATE.PREVIOUS_WK_CAL_WTD       , DIM_DATE.PREVIOUS_WK_CAL_WTD)
                                                                    , PREVIOUS_YR_CAL_WTD         = IFNULL(STAGE_DATE.PREVIOUS_YR_CAL_WTD       , DIM_DATE.PREVIOUS_YR_CAL_WTD)
                                                                    , PREVIOUS_YR_CAL_PTD         = IFNULL(STAGE_DATE.PREVIOUS_YR_CAL_PTD       , DIM_DATE.PREVIOUS_YR_CAL_PTD)
                                                                    , PREVIOUS_YR_CAL_QTD         = IFNULL(STAGE_DATE.PREVIOUS_YR_CAL_QTD       , DIM_DATE.PREVIOUS_YR_CAL_QTD)
                                                                    , PREVIOUS_YR_CAL_YTD         = IFNULL(STAGE_DATE.PREVIOUS_YR_CAL_YTD       , DIM_DATE.PREVIOUS_YR_CAL_YTD)
                                                                    , LAST_UPDATE_DATE            = IFNULL(STAGE_DATE.LAST_UPDATE_DATE          , CURRENT_TIMESTAMP)
                                                     WHEN NOT MATCHED
                                                          THEN INSERT 
                                                               (
                                                                 DATE_KEY
                                                                  , CALENDAR_DATE
                                                                  , DATE_DESC
                                                                  , DAY_NAME
                                                                  , DAY_NAME_ABRV
                                                                  , DAY_IN_WEEK_NO
                                                                  , DAY_IN_MONTH
                                                                  , DAY_STATUS
                                                                  , WEEKDAY_FLAG
                                                                  , WEEKEND_FLAG
                                                                  , WEEK_COMMENCING_DATE_KEY
                                                                  , WEEK_COMMENCING_DATE
                                                                  , WEEK_ENDING_DATE_KEY
                                                                  , WEEK_ENDING_DATE
                                                                  , MONTH_NO
                                                                  , MONTH
                                                                  , MONTH_NAME
                                                                  , MONTH_NAME_ABRV
                                                                  , MONTHEND_FLAG
                                                                  , CAL_DAY_IN_YEAR
                                                                  , CAL_WEEK_IN_YEAR
                                                                  , CAL_QUARTER_NO
                                                                  , CAL_QUARTER
                                                                  , CAL_YEAR
                                                                  , FIN_DAY_IN_YEAR
                                                                  , FIN_WEEK_IN_YEAR
                                                                  , FIN_PERIOD
                                                                  , FIN_QUARTER_NO
                                                                  , FIN_QUARTER
                                                                  , FIN_YEARHALF
                                                                  , FIN_YEAR
                                                                  , FIN_YEAR_NAME
                                                                  , FIN_YEAR_STATUS
                                                                  , FIN_QUARTER_STATUS
                                                                  , FIN_PERIOD_STATUS
                                                                  , FIN_WEEK_STATUS
                                                                  , CURRENT_CAL_WTD
                                                                  , CURRENT_CAL_MTD
                                                                  , CURRENT_CAL_PTD
                                                                  , CURRENT_CAL_QTD
                                                                  , CURRENT_CAL_YTD
                                                                  , PREVIOUS_WK_CAL_WTD
                                                                  , PREVIOUS_YR_CAL_WTD
                                                                  , PREVIOUS_YR_CAL_PTD
                                                                  , PREVIOUS_YR_CAL_QTD
                                                                  , PREVIOUS_YR_CAL_YTD
                                                                  , LAST_UPDATE_DATE
                                                               )
                                                     VALUES
                                                       (
                                                          TO_CHAR(CALENDAR_DATE, 'YYYYMMDD') ::NUMBER
                                                          , CALENDAR_DATE
                                                          , DATE_DESC
                                                          , DAY_NAME
                                                          , DAY_NAME_ABRV
                                                          , DAY_IN_WEEK_NO
                                                          , DAY_IN_MONTH
                                                          , DAY_STATUS
                                                          , WEEKDAY_FLAG
                                                          , WEEKEND_FLAG
                                                          , WEEK_COMMENCING_DATE_KEY
                                                          , WEEK_COMMENCING_DATE
                                                          , WEEK_ENDING_DATE_KEY
                                                          , WEEK_ENDING_DATE
                                                          , MONTH_NO
                                                          , MONTH
                                                          , MONTH_NAME
                                                          , MONTH_NAME_ABRV
                                                          , MONTHEND_FLAG
                                                          , CAL_DAY_IN_YEAR
                                                          , CAL_WEEK_IN_YEAR
                                                          , CAL_QUARTER_NO
                                                          , CAL_QUARTER
                                                          , CAL_YEAR
                                                          , FIN_DAY_IN_YEAR
                                                          , FIN_WEEK_IN_YEAR
                                                          , FIN_PERIOD
                                                          , FIN_QUARTER_NO
                                                          , FIN_QUARTER
                                                          , FIN_YEARHALF
                                                          , FIN_YEAR
                                                          , FIN_YEAR_NAME
                                                          , FIN_YEAR_STATUS
                                                          , FIN_QUARTER_STATUS
                                                          , FIN_PERIOD_STATUS
                                                          , FIN_WEEK_STATUS
                                                          , CURRENT_CAL_WTD
                                                          , CURRENT_CAL_MTD
                                                          , CURRENT_CAL_PTD
                                                          , CURRENT_CAL_QTD
                                                          , CURRENT_CAL_YTD
                                                          , PREVIOUS_WK_CAL_WTD
                                                          , PREVIOUS_YR_CAL_WTD
                                                          , PREVIOUS_YR_CAL_PTD
                                                          , PREVIOUS_YR_CAL_QTD
                                                          , PREVIOUS_YR_CAL_YTD
                                                          , CURRENT_TIMESTAMP
                                                       )                                        
                                                `;
                
                  execute_and_log (sql_populate_dim_date);
                                                         
            } catch (err) {
                log_error(err);
                            } 
 return 'ALL OK';
$$
;
