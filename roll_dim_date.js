-- Procedure to Roll Dim DATE
-- This will update the *_status & the current & previous week/period/quarter/year to date columns.

CREATE OR REPLACE PROCEDURE transform.ROLL_DIM_DATE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // JavaScript code is case sensitive. Input parameter names (not values) are converted to Upper Case unless inside double quotes.
        // Safer to use all variables and parameters in upper case.
        
        var INGEST_DB       = 'PROD_INGEST_DB';         // for logging
        var INGEST_SCHEMA   = 'RAW_BIZ_FILES';               // for logging
        var MODEL_SCHEMA    = 'PRESENTATION';               // for logging
        var MODEL_TABLE     = 'DIM_DATE';                    // for logging
        
        var STAGE_SCHEMA    = 'TRANSFORM';                  // for logging
        var STAGE_TABLE     = 'STAGE_DATE';                    // for logging
        var PROCEDURE_NAME  = 'ROLL_' + MODEL_TABLE;         // for logging
        
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
             // the current date for datawarehouse is day - 1, as per Nandos requirements
             var dw_current_date = 'CURRENT_DATE -1';
                                                         
             var sql_get_curr_fin_year = `SELECT 
                                                fin_year ::number
                                                , fin_quarter_no
                                                , substr(fin_period, 8,2) ::number
                                                , substr(fin_week_in_year, 6,2) ::number
                                            from ${MODEL_SCHEMA}.DIM_DATE 
                                            where calendar_date = ${dw_current_date}
                                            `;
                var result_sql = snowflake.createStatement({sqlText: sql_get_curr_fin_year}).execute();
                result_sql.next();
                
                var curr_fin_year       = result_sql.getColumnValue(1);
                var curr_fin_quarter    = result_sql.getColumnValue(2);
                var curr_fin_period     = result_sql.getColumnValue(3);
                var curr_fin_week       = result_sql.getColumnValue(4);

                var sql_roll_dates = `UPDATE ${MODEL_SCHEMA}.DIM_DATE
                                                SET day_status  = case 
                                                                        when CALENDAR_DATE = ${dw_current_date}     then 'Today'
                                                                        when CALENDAR_DATE = ${dw_current_date} -1  then 'Previous Day'
                                                                        when CALENDAR_DATE = ${dw_current_date} -2  then '2 Days Ago'
                                                                        when (mod(year(CALENDAR_DATE) - 1, 4) = 0 and CALENDAR_DATE = ${dw_current_date} -365)
                                                                                OR (mod(year(CALENDAR_DATE) - 1, 4) <> 0 and CALENDAR_DATE = ${dw_current_date} -364) 
                                                                                                                    then 'Previous Day Last Year'
                                                                        else 'Other' end
                                                , fin_year_status = case when fin_year = ${curr_fin_year} then 'Current Year'
                                                                        when fin_year = ${curr_fin_year} -1 then 'Previous Year'
                                                                        when fin_year = ${curr_fin_year} -2 then '2 Year Back'
                                                                        else NULL end
                                                , fin_quarter_status = case when fin_year = ${curr_fin_year} and fin_quarter_no = ${curr_fin_quarter} then 'Current Quarter'
                                                                        when fin_year = ${curr_fin_year} and fin_quarter_no = ${curr_fin_quarter} -1 then 'Previous Quarter'
                                                                        when fin_year = ${curr_fin_year} -1 and fin_quarter_no = ${curr_fin_quarter} then 'Current Quarter Last Year'
                                                                        else NULL end
                                                , fin_period_Status = case when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} then 'Current Period'
                                                                        when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} -1 then 'Previous Period'
                                                                        when fin_year = ${curr_fin_year} -1 and substr(fin_period, 8,2) = ${curr_fin_period} then 'Current Period Last Year'
                                                                        else NULL end
                                                , fin_week_status = case when fin_year = ${curr_fin_year} and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} then 'Current Week'
                                                                        when fin_year = ${curr_fin_year} and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} -1 then 'Previous Week'
                                                                        when fin_year = ${curr_fin_year} and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} -2 then '2 Weeks Back'
                                                                        -- last year
                                                                        when fin_year = ${curr_fin_year} -1 and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} then 'Current Week Last Year'
                                                                        when fin_year = ${curr_fin_year} -1 and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} -1 then 'Previous Week Last Year'
                                                                        when fin_year = ${curr_fin_year} -1 and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} -2 then '2 Weeks Back Last Year'
                                                                        -- 2 year back
                                                                        when fin_year = ${curr_fin_year} -2 and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} then 'Current Week 2 Year Back'
                                                                        when fin_year = ${curr_fin_year} -2 and substr(fin_week_in_year, 6, 2) ::number = ${curr_fin_week} -1 then 'Previous Week 2 Year Back'    
                                                                        else NULL end
                                                // Current period spans
                                                , current_cal_wtd = case when last_day(${dw_current_date}, 'week') - 6 <= DIM_DATE.calendar_date
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} then 'YES' else 'NO' end
                                                , current_cal_mtd = case when month = to_char(${dw_current_date}, 'YYYY-MM') 
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} then 'YES' else 'NO' end
                                                , current_cal_ptd = case when fin_year = ${curr_fin_year} and substr(fin_period, 8,2) = ${curr_fin_period} 
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} then 'YES' else 'NO' end
                                                , current_cal_qtd = case when cal_quarter = year(${dw_current_date})::varchar || '0'|| quarter(${dw_current_date})::varchar 
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} then 'YES' else 'NO' end
                                                , current_cal_ytd = case when cal_year = year(${dw_current_date}) 
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} then 'YES' else 'NO' end
                                                // Previous period spans
                                                , previous_wk_cal_wtd = case when last_day(${dw_current_date} -7, 'week') - 6 <= DIM_DATE.calendar_date
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} -7 then 'YES' else 'NO' end
                                                , previous_yr_cal_wtd = case when last_day(${dw_current_date} - 364, 'week') - 6 <= DIM_DATE.calendar_date 
                                                                                and DIM_DATE.calendar_date <= ${dw_current_date} - 364 then 'YES' else 'NO' end
                                                , previous_yr_cal_ptd = case when fin_year = ${curr_fin_year} -1 and substr(fin_period, 8,2) = ${curr_fin_period}
                                                                                and to_char(DIM_DATE.calendar_date,'mmdd') <= to_char(${dw_current_date}, 'mmdd') then 'YES' else 'NO' end
                                                , previous_yr_cal_qtd = case when cal_quarter = (year(${dw_current_date}) -1) ::varchar || '0'|| quarter(${dw_current_date})::varchar 
                                                                                and to_char(DIM_DATE.calendar_date,'mmdd') <= to_char(${dw_current_date}, 'mmdd') then 'YES' else 'NO' end
                                                , previous_yr_cal_ytd = case when cal_year = year(${dw_current_date}) - 1 
                                                                                and to_char(DIM_DATE.calendar_date,'mmdd') <= to_char(${dw_current_date}, 'mmdd') then 'YES' else 'NO' end
                                                , LAST_UPDATE_DATE = CURRENT_TIMESTAMP
                                        `;
                execute_and_log (sql_roll_dates);
                
        } catch (err) {
                log_error(err);
                            } 
return 'ALL OK';
$$
;