/*
Building a data-pipeline – HANDS-on activity

Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

In this exercise, we will build a small data pipeline which will

    1. extract file from a stage and load data into a staging table using pipe.
    2. transform the data by aggregating the data from staging to target table using streams.
    3. automate the transformation process using task

*/

--------------- Prepare the staging location with data downloaded from an existing table (people) 
--------------- which we created in a previous session.

-- Create a named internal stage to hold the file containing data from a table
-- We will associate the file format in-line (i.e. at runtime) while creating the stage.
-- All the records downloaded into this stage will be pipe separated
CREATE OR REPLACE STAGE <corpid>_datapipeline_stg
FILE_FORMAT = (type = 'csv' 
             field_delimiter = '|'
            ) ;

-- Create an staging table to load for the people data from staging.
-- Note: Tempoary table CANNOT be used in the pipe definition
CREATE TABLE <corpid>_people_details
(
    name       VARCHAR2,
    gender     CHAR(1),
    age        NUMBER,
    height_in  NUMBER,
    weight_lbs NUMBER
); 

-- We will create a SNOWPIPE to move data from the staging location to the staging table we created previously.
-- Here, note that we have used AUTO_INGEST = FALSE. 
-- When building a pipe for loading data from internal stage, the value of AUTO_INGEST MUST BE FALSE
-- AUTO_INGEST = TRUE is used to load files from external stage (like S3) automatically, via the SNS service.
CREATE OR REPLACE PIPE <corpid>_datapipe 
AUTO_INGEST = FALSE
AS
  COPY INTO <corpid>_people_details
  FROM @<corpid>_datapipeline_stg;

-- List the pipes' details
SHOW PIPES LIKE '<corpid>_datapipe';

-- Display the current state of the pipe
SELECT SYSTEM$PIPE_STATUS('<corpid>_datapipe');

-- Let us create a stream to track changes between two transactional points on the <corpid>_people_details table.
-- Note as are using APPEND_ONLY = TRUE. This means that this stream will register only inserted records.
-- This is genreally used for a typical ELT process, where the staging-table will be truncated before the next load takes place.
CREATE OR REPLACE STREAM <corpid>_people_details_stream 
ON TABLE <corpid>_people_details 
append_only = TRUE;

-- The stream structure of the table will be based on the table that is it created on.
-- Note the additional three metadata columns in the stream.
-- Question : Will a stream contain records of the base table?
SELECT * FROM <corpid>_people_details_stream;

-- We will create a summary table that will contain the transformed data from the staging-table.
-- The transformation process will aggregate the data from the <corpid>_people_details table 
-- whenever there is an insert operation.
CREATE OR REPLACE TABLE <corpid>_people_info_summary
(gender     CHAR(1),
 people_cnt NUMBER,
 avg_age    NUMBER,
 avg_height NUMBER,
 avg_weight NUMBER
);

-- Now, we will automate the transformation logic within a TASK which will run every minute
-- AND use a user-defined warehouse to execute the transformation logic.
-- The task will get execute the transformation logic ONLY when there is a data in the stream as defined
-- in the WHEN CLAUSE. 
-- This saves credits as no warehouse compute is used even if the task gets triggered every minute.

-- The transformation logic is a simple merge statement which reads data from the stream.
CREATE OR REPLACE TASK <corpid>_transform_task
WAREHOUSE='compute_wh'
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('<corpid>_people_details_stream')
AS
    MERGE INTO <corpid>_people_info_summary tgt
    USING ( SELECT gender, 
                   COUNT(1) people_cnt,
                   AVG(age) avg_age,
                   AVG(height_in) avg_height,
                   AVG(weight_lbs) avg_weight
           FROM <corpid>_people_details_stream
           GROUP BY gender
          ) src
          ON tgt.gender = src.gender
          WHEN MATCHED THEN
                 UPDATE SET tgt.people_cnt = src.people_cnt,
                            tgt.avg_age = src.avg_age,
                            tgt.avg_height = src.avg_height,
                            tgt.avg_weight = src.avg_weight
         WHEN NOT MATCHED THEN
                INSERT ( gender     ,
                         people_cnt ,
                         avg_age    ,
                         avg_height ,
                         avg_weight )
                 VALUES (src.gender     ,
                         src.people_cnt ,
                         src.avg_age    ,
                         src.avg_height ,
                         src.avg_weight);


-- Please note that when a task is created it will always be in a SUSPENDED state.
-- The user has to RESUME the TASK once after its created. We will resume the task in the next few steps.
SHOW TASKS LIKE '<corpid>_transform_task';

------------- Let us trigger the data pipeline we just built ------------------------------

-- Issue the COPY statement to select data from people table.
-- The selected fields should be delimited by "|"s when downloaded.
-- Hence, associate the csv_type file_format to the COPY statement.
COPY INTO @datapipeline_stg
FROM <corpid>_people;

-- List the files downloaded into the stage
ls @<corpid>_datapipeline_stg;

-- Display the records in the stage.
SELECT $1, $2, $3, $4, $5 FROM @<corpid>_datapipeline_stg;


-- Ideally, we will be invoking the SNOWPIPE REST API endpoint using a python or a java script.
-- However, for this lab, let us perform a small workaround and manually force the pipe to execute.
-- Question: Will the snowpipe consume user defined warehouse compute credits?
ALTER PIPE <corpid>_datapipe REFRESH;

-- After the pipe has been invoked (either manually, via API or automatically), typically, we will need to wait 
-- for close to a minute for the COPY command in the pipe to be executed.
SELECT SYSTEM$PIPE_STATUS('<corpid>_datapipe');

-- After around a minute, the staging-table should be populated.
SELECT * FROM <corpid>_people_details;

-- Simulatenously, the stream also should be populated.
-- Notice that for the new records which get inserted into the base table, 
-- the stream will have "METADATA$ACTION" value as "INSERT"
SELECT * FROM <corpid>_people_details_stream;

-- Now, let is force the TASK to resume.
ALTER TASK <corpid>_transform_task RESUME;

-- After a minute, the task will trigger the transformation logic
-- and the aggregated data will be populated in the summary table
SELECT * FROM <corpid>_people_info_summary;


-- CONGRATULATIONS!! You have just built an end-end data pipeline.
