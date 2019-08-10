#!/bin/ksh

snowsql -a <> -u <> <<EOF

USE warehouse SF_TUTS_WH;

USE database MYDB;

use schema private;

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
RECORD_DELIMITER='\\n'
FIELD_DELIMITER='|'
TRIM_SPACE = FALSE;

CREATE OR REPLACE STAGE stg_data_csv
file_format = my_csv_format;

PUT file://./departments.csv @stg_data_csv;

CREATE OR REPLACE TABLE departments 
(DEPARTMENT_ID int, DEPARTMENT_NAME string, MANAGER_ID int, LOCATION_ID int);

COPY INTO departments from @stg_data_csv/departments.csv.gz;

EOF

