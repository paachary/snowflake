/*
Cloning – HANDS-on activity
---------------------------------------

Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.
Some activities like creating a database requires you to have privileges,
which might not be currently available to your office account. 
You can follow along and try out these activities in your personal account outside of office network.

 Assume that our current database is a production database.
 
 We would like to take copies of that for our development, QA and Performance teams as of its current state.
 
 If this were an Oracle database, then this activity would have been a tedious task.
 
 Let's start by cloning our database as of its current state and create a development database.
 
 After refreshing the objects' pane in the UI (left pane), do you see the database and all its schema? How easy was it?
 
 How would you know its only metadata operation and no extra storage was created at the time of cloning?
 
 Let's try to prove this point by cloning a table in the current database.
 
 First, let us create a new table "metrics" and insert few records into it.
 
 Query the table columns along with the partition_names and note the partition_name(s) separately.
 
 Let's create a clone of this table and query its contents along with the partition_names.
 
 What do you notice?
 
 Let us update one record in the cloned table and again query its contents along with the partition_names.
  
 What do you notice?
 
 You can use cloning with Time-Travel effectively to take regular backups, create environments and ensure platform stability and 
 a perfect DR plan for any unforeseen disaster.
 
 */

-- Creating a database from an existing database using the clone feature.
CREATE DATABASE dev_db CLONE training_db;

------------- CLONING INDIVIDUAL OBJECTS FOR TESTING / BACKUP / RECOVERY ----------------------------

-- Create a new table
CREATE TABLE <corp_id>_metrics ( metric_id NUMBER, metric_name VARCHAR);

-- Populate the table
INSERT INTO <corp_id>_metrics (metric_id, metric_name) VALUES
( 1, 'CPU_PER_CALL'),
( 2, 'CPU_PER_SESSION'),
( 3, 'MEMORY_USED'),
( 4, 'MEMORY_AVAILABLE');

-- Note down the file names for the records in the table
SELECT metadata$partition_name, metric_id, metric_name FROM <corp_id>_metrics;

-- Create a new table which is cloned from the earlier table
CREATE TABLE <corp_id>_metrics_cloned CLONE <corp_id>_metrics;

-- Now note down the file names for the records in the table.
-- What do you see?
SELECT metadata$partition_name, metric_id, metric_name FROM <corp_id>_metrics_cloned;

-- Update one record of the cloned table
UPDATE <corp_id>_metrics_cloned SET metric_name='MEMORY_USED_MB' WHERE metric_id = 3;

-- Again, note down the file names for the records in the table.
-- Do you see the file names same as the earlier ones or have they changed?
-- Why do you think that is the case?
SELECT metadata$partition_name, metric_id, metric_name FROM <corp_id>_metrics_cloned;