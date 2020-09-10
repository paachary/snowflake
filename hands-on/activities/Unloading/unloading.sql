/*
Unloading – HANDS-on activity
----------------------------

Activities:
============
Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

COPY command can be used to load data to and from a database table.

In this exercise, we will unload data from a table containing JSON data into a CSV file with fields separated by "|"s.

Please note that the COPY command will dump the data into a file in a stage location.

For downloading the file, we will need to use SNOWSQL commandline interface. We will skip the step 
of downloading the file in this exercise.

First we will create an internal named stage with the CSV file format to hold the data file.

Then we issue the COPY command to dump data from a table using a SQL statement into a file in the stage location.

We then list the stage to ensure we see the file.

How is the file stored in the stage location?

*/

-- Create a named internal stage to hold the file containing data from a table
-- Associate the previously created csv_type file_format to this stage.
-- All the records downloaded into this stage will be pipe separated
CREATE OR REPLACE STAGE <corp_id>_internal_stg
FILE_FORMAT = <corp_id>_csv_type;

-- Issue the COPY statement to select data from devices table.
-- The selected fields should be delimited by "|"s when downloaded.
-- Hence, associate the csv_type file_format to the COPY statement.
COPY INTO @<corp_id>_internal_stg/devices
FROM ( SELECT device_info:device_type::VARCHAR,
       value:f::NUMBER,
       value:t::NUMBER,
       value:v:ACHZ::NUMBER
FROM <corp_id>_devices,
LATERAL FLATTEN(input => device_info:events))
FILE_FORMAT = <corp_id>_csv_type;

-- List the stage to review the listings:
ls @<corp_id>_internal_stg;

-- Use the get command on the SNOWSQL CLI to retreive this file to the local disk
