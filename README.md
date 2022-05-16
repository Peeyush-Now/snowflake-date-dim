# Date and Time of the Day dimension tables for Snowflake Cloud Data Platform
DDL Scripts and procedures for building and maintaining dates on snowflake. The procedures are in JavaScript as the SQL based stored procedures were quite limited in capability when this was written. 
The date dimension script uses a Start Date and End Date value to generate  the number of days between them. A flag can either repopulate or append to an existing date dimension table values. 

There is a rollover procedure that can be scheduled as a task to keep updating the current date and associated timeperiods. 
Setup date rollover as a snowflake task as below
```
CREATE OR REPLACE {{ schema }}.TASK TASK_ROLL_DIM_DATE 
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON 0 3 * * *  Pacific/Auckland'
USER_TASK_TIMEOUT_MS = 900000 -- 15 minutes
COMMENT = 'Daily Task for rolling date dimension'
AS
 CALL {{ schema }}.ROLL_DIM_DATE();
;
```
There is an execute & logging procedure for snowflake JS stored procedures and some ddls for these tables.
If you find a bug, ask nicely and I will attempt to fix it.

