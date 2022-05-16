//call execute_and_log('test_procedure', 'count_step', 'select count(*) from fact_sales');

use database {{ Database }};
use schema {{ schema }};

create or replace procedure execute_and_log(PROCEDURE_NAME string, STEP_NAME string, SQL_STATEMENT string)
    returns string
    language javascript
    comment = 'Execute statement and store result in audit table if successful, or error table if not'
    execute as owner
    as
    $$

    try {
        var result_set = snowflake.createStatement( {sqlText: SQL_STATEMENT} ).execute()
        result_set.next();
        var result_message = result_set.getColumnName(1) + ': ' + result_set.getColumnValue(1);

        // log step output in audit table
        snowflake.execute({
            sqlText: `insert into presentation.audit_table (procedure_name, sql_step, step_result, audit_timestamp) values (?,?,?,current_timestamp)`,
            binds: [PROCEDURE_NAME, STEP_NAME, result_message]
        });

        return result_message;   // Return a success/error indicator.
    }
    catch (err)  {
        // log error message in error table
        snowflake.execute({
            sqlText: `insert into presentation.error_table (procedure_name, sql_step, error_code, error_message, error_state, stack_trace, error_timestamp) values (?,?,?,?,?,?,current_timestamp)`,
            binds: [PROCEDURE_NAME, STEP_NAME, err.code, err.message, err.state, err.stackTraceTxt]
        });
        return STEP_NAME + " failed: " + err.message;   // Return a success/error indicator.
    }
    $$
;