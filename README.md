# Date and Time of the Day dimension tables for Snowflake Cloud Data Platform
DDL Scripts and procedures for building and maintaining dates on snowflake. The procedures are in JavaScript because the SQL based stored procedures are limited in capability. 
The date dimension script look back anf forth X years from the current date to populate the table. 
There is a rollover procedure that can be scheduled as a task to keep updating the current date and associated timeperiods. 

Happy to receive feedback on improvements and bugs.

