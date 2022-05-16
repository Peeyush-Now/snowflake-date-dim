use database {{ Database }};
use schema {{ schema }};

-- create a resuable audit table, log last (edw) process date and records processed, among other things
CREATE OR REPLACE TABLE presentation.AUDIT_TABLE (
  AUDIT_KEY             NUMBER AUTOINCREMENT
  , PROCEDURE_NAME      STRING
  , SQL_STEP            STRING
  , STEP_RESULT         STRING
  , SCHEMA_NAME         STRING
  , TABLE_NAME          STRING
  , AUDIT_TIMESTAMP     TIMESTAMP_NTZ(9)
		);

CREATE OR REPLACE TABLE presentation.DW_PROCESS_DATE (
  TABLE_NAME                STRING
  , SCHEMA_NAME             STRING
  , LAST_PROCESS_DATE       TIMESTAMP_NTZ(9)
  , LAST_PROCESS_ROWCOUNT   NUMBER
  , LAST_UPDATE_DATE        TIMESTAMP_NTZ(9)
  , COUNTRY VARCHAR(20)
		);

-- create an Error Logging TABLE
CREATE OR REPLACE TABLE  presentation.ERROR_TABLE (
  ERROR_KEY             NUMBER AUTOINCREMENT 
  , PROCEDURE_NAME      STRING
  , SQL_STEP            STRING
  , ERROR_CODE          NUMBER
  , ERROR_MESSAGE       STRING
  , ERROR_STATE         STRING
  , STACK_TRACE         STRING
  , ERROR_TIMESTAMP     TIMESTAMP_NTZ(9)
		);