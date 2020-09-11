/*
Cluster and Caching – HANDS-on activity
------------------------------------------
Activities:
============
Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

In this exercise, we will be creating one large un-clustered table and one large clustered table, with clustering key on a date column.
We will review the query-profile for queries against these tables a range of dates.

Next, we will also experiment on how Snowflake enhances performances using all the three types of caches.

*/

---- Creating a seed table from which we will create the required large tables for the exercise

CREATE OR REPLACE TABLE <corpid>_transactions
AS
    SELECT DATEADD( day, '-' || ROW_NUMBER() OVER (ORDER BY null),
                        DATEADD(day, '+1', current_date())) AS transaction_date,
          ABS(MOD(RANDOM(1), 10000)) +1 AS customer_id,
          ABS(MOD(RANDOM(1), 21010)) +1 AS transaction_id,
          ABS(MOD(RANDOM(1), 100)) +1 AS amount  
    FROM TABLE (GENERATOR(rowcount => 100));


-- Creating the large un-clustered table based on the seed table

CREATE OR REPLACE TABLE <corpid>_transactions_large
AS 
SELECT a.transaction_date + MOD(RANDOM(), 2000) AS transaction_date, 
       a.Customer_ID, 
       RANDOM() AS transaction_id, 
       a.amount 
FROM <corpid>_transactions a CROSS JOIN 
     <corpid>_transactions b CROSS JOIN 
     <corpid>_transactions c CROSS JOIN 
     <corpid>_transactions d ;

-- Creating the clustered large table with cluserting key on transaction date

CREATE TABLE <corpid>_transactions_large_clustered CLUSTER BY (transaction_date)
AS SELECT * FROM <corpid>_transactions_large;

ALTER SESSION SET QUERY_TAG='<corpid>_query'; 

-- Execute this SQL and observe the Query-profile from the History tab.
SELECT COUNT(1) FROM <corpid>_transactions_large 
WHERE transaction_date BETWEEN DATE '2016-01-01' AND DATE '2016-03-31';

-- Navigate to the history tab and filter based on query_tag with the value you set above.

-- Now, execute the same SQL in succession twice and review the Query-profile from the History tab.

-- What are your observations?

-- Now execute the count SQL query on the clustered table and observe the Query-profile from the History tab.
SELECT COUNT(1) FROM <corpid>_transactions_large_clustered 
WHERE transaction_date BETWEEN DATE '2016-01-01' AND DATE '2016-03-31';

-- Navigate to the history tab and filter based on query_tag with the value you set above.

-- Now, execute the same SQL in succession twice and review the Query-profile from the History tab.

-- What are your observations?

-- Querying Metadata CACHE

-- Execute this query
-- Navigate to the history tab
-- From the record for this query in the history tab, what do you infer?
-- Navigate to the Query-Profile.
-- What do you infer?
SELECT MIN(transaction_date),
       MAX(transaction_date),
       COUNT(1) 
FROM <corpid>_transactions_large;

SHOW TABLES LIKE '<corpid>_transactions%';
-- Now, execute the following SHOW statement
-- From the record for this query in the history tab, what do you infer?
-- Navigate to the Query-Profile.
-- What do you infer?

