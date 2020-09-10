/*
Time-Travel – HANDS-on activity
---------------------------------------

Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

Lets try to get the record prior to the update.

Get the SQL id of the query before the update was made.

How do you get that?

Given the SQL ID, query the table at that point in time before the update was made.

Note down the partition_name as well for the record. 

Do you see the previous value of the name column and partition_name prior to the update?

Try dropping the table and retrieve it back?

Are you able to get it back?

How will you benefit from this feature?

How cool was that?

*/

SELECT query_id,query_text 
FROM TABLE(information_schema.query_history())
WHERE query_text LIKE 'UPDATE <corp_id>_table_perm SET name%'; -- Retrieve the query_id

SELECT metadata$partition_name , id, name 
FROM <corp_id>_table_perm BEFORE(statement => '<query_id>') 
WHERE id = 1; -- Do you see the partition_name same as the one that was noted before the earlier update took place?

CREATE TABLE <corp_id>_table_perm_backup CLONE <corp_id>_table_perm BEFORE(statement => '<query_id>') ;

DROP TABLE <corp_id>_table_perm;

UNDROP TABLE <corp_id>_table_perm;

DESC TABLE <corp_id>_table_perm;

SHOW TABLES LIKE '<corp_id>_table_perm';