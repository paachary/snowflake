/*
Tables – HANDS-on activity
----------------------------

Activities:
============
Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

You will create one table each of the three types and describe them.

How do you find out which type it is?

You will then load data into each of the three tables using one worksheet of the GUI.

Open a new worksheet and use the same role and database and warehouse as the earlier worksheet.

Query each of the tables.

What do you expect and what do you get?

Which is the 4th type table you didn’t create here?


=================================================================================================================
*/

-- Create a permanent table
CREATE TABLE <corp_id>_table_perm ( id number, name varchar);

-- Create a temporary table
CREATE  TEMP TABLE <corp_id>_table_temp ( id number, name varchar);

-- Create a transient table
CREATE TRANSIENT TABLE <corp_id>_table_transient ( id number, name varchar);
-------------------------------------------------------

SHOW TABLES like 'table_%';

-------------------------------------------------------
-- Insert values into the three tables
INSERT INTO <corp_id>_table_perm VALUES (1,'Prashant'), (2, 'Alan');
                             
INSERT INTO <corp_id>_table_temp VALUES (1,'Prashant'), (2, 'Alan');
                             
INSERT INTO <corp_id>_table_transient VALUES (1,'Prashant'), (2, 'Alan');

--------------------------------------

-- Execute these SQLs using a new worksheet.
-- Use the same role and database and warehouse as the earlier worksheet.

SELECT * FROM <corp_id>_table_perm;

SELECT * FROM <corp_id>_table_temp;

SELECT * FROM <corp_id>_table_transient;

-- What results do you get?
