/*
Micro-partitions – HANDS-on activity
---------------------------------------

Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

Query the table “table_perm” for one id and note down the partition_name

Next, update the name of the id in that table and again note down the partition_name name.

What do you see?

What do you infer?

=============================================================
*/

SHOW TABLES LIKE '<corp_id>_table_%';


SELECT metadata$partition_name , id, name FROM <corp_id>_table_perm; -- note down the partition name

UPDATE <corp_id>_table_perm SET name = 'Peter' WHERE id = 1;

SELECT metadata$partition_name , id, name FROM <corp_id>_table_perm WHERE id = 1; -- note down the partition name
