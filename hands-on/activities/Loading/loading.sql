/*
Loading – HANDS-on activity
----------------------------

Activities:
============
Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

We will be using GUI to upload the files into the target tables.

Use GUI to upload small files or files with less size.

For files larger than 50 MB, use SNOWSQL command-line interface and issue the "put" command.

For this exercise, we will upload three files: people.csv a CSV with | delimiter 
along with two json files, sales.json & devices.json.

All these files are available for you to download.

First create a file format for the csv file. Then create the corresponding target table.

Load the table with the csv file from GUI. 

Before the final “upload” activity on the GUI, click on Show SQL button.

    1. What kind of stage is being used to load the file?

    2. What is the COPY statement syntax?

    3. Can you use complex SQL within the COPY statement?

    4. What does “purge” option in the COPY statement do?

Next, create another file format to upload 2 separate json files (sales.json and devices.json). 

Create the corresponding target tables called sales and devices with a variant datatype column each.

Variant datatypes are used to store semi-structured data. Examples: json, parquet and avro

Follow the examples below to understand how to query specific elements from the variant datatype.

*/

-------------- LOADING CSV FILE ---------------------------

-- Create a file format to match the type of file we will be uploading.
-- This file is a pipe delimited one. It also has a column which is enclosed with double quotes (")

CREATE OR REPLACE FILE FORMAT <corp_id>_csv_type
TYPE = csv
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = '|'
SKIP_HEADER = 1;

-- Create the target table into which the file contents will be loaded.
CREATE TABLE <corp_id>_people
(
    name       VARCHAR2,
    gender     CHAR(1),
    age        NUMBER,
    height_in  NUMBER,
    weight_lbs NUMBER    
); 

-- Use the GUI to load the file into the target table.
-- Note that when file is getting uploaded via internal stages, snowflake automatically compresses and encrypts the file.
-- All data in snowflake is encrypted.

-- Once uploaded, query the target table.
SELECT * FROM <corp_id>_people;


-------------- LOADING A SIMPLE JSON FILE ---------------------------

-- Create a file format to load a json file.

CREATE OR REPLACE FILE FORMAT <corp_id>_json_type
TYPE = json
STRIP_OUTER_ARRAY = TRUE;

-- Create the target table with a variant column into which the file contents will be loaded.
CREATE TABLE <corp_id>_sales
(
    sales_info VARIANT
); 

-- Use the GUI to load the file (sales.json) into the target table (sales).
-- Note that when file is getting uploaded via internal stages, snowflake automatically compresses and encrypts the file.
-- All data in snowflake is encrypted.

-- Once uploaded, query the target table. The table contains one column which holds each record.
SELECT * FROM sales;

-- To access top level attributes and scalar attributes from the sales table, we can use javascript notation (:) as shown below.
SELECT sales_info:price,
       sales_info:sale_date,
       sales_info:location:state_city,
       sales_info:location:zip       
FROM <corp_id>_sales;


-- Note that the datatypes displayed in the output are in json native datatype.
-- To cast them into the appropriate snowflake native datatype, following the notation below
SELECT sales_info:price::NUMBER,
       sales_info:sale_date::DATE,
       sales_info:location:state_city::VARCHAR,
       sales_info:location:zip::VARCHAR
FROM <corp_id>_sales;

-----------------------------------------------------------------

-------------- LOADING A NESTED JSON FILE ---------------------------
-- Create the target table with a variant column into which the file contents will be loaded.

CREATE TABLE <corp_id>_devices
(
    device_info VARIANT
); 

-- Use the GUI to load the file (devices.json) into the target table (devices).
-- We will reuse the JSON file format (json_type) we created in the earlier step.
-- Note that when file is getting uploaded via internal stages, snowflake automatically compresses and encrypts the file.
-- All data in snowflake is encrypted.

-- Once uploaded, query the target table. The table contains one column which holds each record.
SELECT * FROM <corp_id>_devices;

-- The json record contains a non-scalar "events" attribute (a list) which nests multiple levels.
-- There are two methods of fetching data from non-scalar attributes.
-- 1. is the javanotation along with the array element; example: device_info:events[0]:f
--    This does not scale well if there are multiple array elements.
-- 2. is the use of FLATTEN table function which explodes the array into multiple records.
-- Both of these methods are depicted below:

-- Method 1 (non-optimal method)
SELECT device_info:device_type,
       device_info:events[0]:f,
       device_info:events[0]:t,
       device_info:events[0]:v:ACHZ,
       device_info:events[1]:f,
       device_info:events[1]:t,
       device_info:events[1]:v:ACHZ
FROM <corp_id>_devices;


-- Method 2 (optimal method)
SELECT device_info:device_type::VARCHAR,
       value:f::NUMBER,
       value:t::NUMBER,
       value:v:ACHZ::NUMBER
FROM <corp_id>_devices,
LATERAL FLATTEN(input => device_info:events);

-- LATERAL join is equivalent to a CARTESIAN PRODUCT JOIN
-- Loosely, it means that a LATERAL join is like a SQL foreach loop 
-- in which Snowflake will iterate over each row in a result set  
