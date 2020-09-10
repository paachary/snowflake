/*
Views – HANDS-on activity
----------------------------

Activities:
============
Note: Ensure you have a database, a schema and warehouse selected in the GUI before starting this activity.

You will create one view of each of types that we learnt in the previous section.

How do you find out which type each is?

You will start by creating two base tables: emp and dept.

Note that the emp table represents a SCD Type 2 dimension table, i.e., 
we pick up the latest record if the “last_update_dt” value is NULL

What difference do you see in creating simple view v/s materialized view?

What difference do you see in creating recursive  view v/s materialized view?

Will there be a performance difference in querying the "recursive view with base tables" & "recursive view with materialized view"?
Why do you think so?

How do you differentiate between a non-materialized view v/s materialized view?

How different are secure views?

Why are they called secure views?
=================================================================================================================
*/


--------- CREATING THE BASE-TABLES -----------------------

-- Creating the deptarment master data
CREATE TABLE <corp_id>_dept(  
  deptno     NUMBER(2,0) NOT NULL,  
  dname      VARCHAR2(14) NOT NULL,  
  loc        VARCHAR2(13) NOT NULL,  
  CONSTRAINT PK_DEPT PRIMARY KEY (DEPTNO)  
);

-- Creating the employee master data (dimension table), containing few  sensitive columns such as salary and commission.
-- This table also stores hierarchy amongst the manager and their associates.
CREATE TABLE <corp_id>_emp(  
  emp_key         NUMBER      NOT NULL,
  empno           NUMBER(4,0) NOT NULL,  
  ename           VARCHAR2(10) NOT NULL,  
  job             VARCHAR2(9) NOT NULL,  
  mgr             NUMBER(4,0),  
  hiredate        DATE NOT NULL,  
  sal             NUMBER(7,2) NOT NULL,  
  comm            NUMBER(7,2),  
  deptno          NUMBER(2,0) NOT NULL,  
  first_update_dt DATE NOT NULL,
  last_update_dt  DATE,
  CONSTRAINT pk_emp PRIMARY KEY (emp_key),  
  CONSTRAINT fk_deptno FOREIGN KEY (deptno) REFERENCES dept (deptno)  
);

--------- POPULATING THE BASE-TABLES -----------------------

-- Inserting data into deptartments table
INSERT INTO <corp_id>_dept (deptno, dname, loc)
VALUES
    (10, 'ACCOUNTING', 'NEW YORK'),
    (20, 'RESEARCH', 'DALLAS'),
    (30, 'SALES', 'CHICAGO'),
    (40, 'OPERATIONS', 'BOSTON');

-- Inserting data into employee table.
INSERT INTO <corp_id>_emp  
VALUES
    ( 1,7839, 'KING',   'PRESIDENT', null,  to_date('17-11-1981', 'dd-mm-yyyy'),  5000, null, 10 , current_date()- (365*10),current_date()- (365*8) ),
    ( 2,7839, 'KING',   'PRESIDENT', null,  to_date('17-11-1981', 'dd-mm-yyyy'),  5000, null, 10 , current_date()- (365*8),current_date()- (365*6) ),
    ( 3,7839, 'KING',   'PRESIDENT', null,  to_date('17-11-1981', 'dd-mm-yyyy'),  5000, null, 10 , current_date()- (365*6),null ),
    ( 4,7698, 'BLAKE',  'MANAGER',   7839,  to_date('1-5-1981',   'dd-mm-yyyy'),  2850, null, 30, current_date()- (365*10),current_date()- (365*8) ),
    ( 5,7698, 'BLAKE',  'MANAGER',   7839,  to_date('1-5-1981',   'dd-mm-yyyy'),  2850, null, 30 ,current_date()- (365*8),current_date()- (365*6) ),
    ( 6,7698, 'BLAKE',  'MANAGER',   7839,  to_date('1-5-1981',   'dd-mm-yyyy'),  2850, null, 30 ,current_date()- (365*6),null ), 
    ( 7,7782, 'CLARK',  'MANAGER',   7839,  to_date('9-6-1981',   'dd-mm-yyyy'),  2450, null, 10 ,current_date()- (365*6),null  ),
    ( 8,7566, 'JONES',  'MANAGER',   7839,  to_date('2-4-1981',   'dd-mm-yyyy'),  2975, null, 20 ,current_date()- (365*6),null ),
    ( 9,7788, 'SCOTT',  'ANALYST',   7566,  to_date('13-07-1987', 'dd-mm-yyyy'),  3000, null, 20 , current_date()- (365*6),null),
    ( 10,7902, 'FORD',   'ANALYST',   7566,  to_date('3-12-1981',  'dd-mm-yyyy'),  3000, null, 20 ,current_date()- (365*6),null ),
    ( 11,7369, 'SMITH',  'CLERK',     7902,  to_date('17-12-1980', 'dd-mm-yyyy'),  0, null, 20,current_date()- (365*6),null  ),
    ( 12,7499, 'ALLEN',  'SALESMAN',  7698,  to_date('20-2-1981',  'dd-mm-yyyy'),  1600, 300, 30 ,current_date()- (365*6),null  ),
    ( 13,7521, 'WARD',   'SALESMAN',  7698,  to_date('22-2-1981',  'dd-mm-yyyy'),  1250, 500, 30 ,current_date()- (365*6),null  ),
    ( 14,7654, 'MARTIN', 'SALESMAN',  7698,  to_date('28-9-1981',  'dd-mm-yyyy'),  1250, 1400, 30 ,current_date()- (365*6),null ),
    ( 15,7844, 'TURNER', 'SALESMAN',  7698,  to_date('8-9-1981',   'dd-mm-yyyy'),  1500, 0, 30  ,current_date()- (365*6),null ),
    ( 16,7876, 'ADAMS',  'CLERK',     7788,  to_date('13-7-1987',  'dd-mm-yyyy'),  1100, null, 20,current_date()- (365*6),null ),
    ( 17,7900, 'JAMES',  'CLERK',     7698,  to_date('3-12-1981',  'dd-mm-yyyy'),  950, null, 30 ,current_date()- (365*6),null ),
    ( 18,7934, 'MILLER', 'CLERK',     7782,  to_date('23-1-1982',  'dd-mm-yyyy'),  1300, null, 10 ,current_date()- (365*8),current_date()- (365*6) ),
    ( 19,7934, 'MILLER', 'CLERK',     7782,  to_date('23-1-1982',  'dd-mm-yyyy'),  1300, null, 10 ,current_date()- (365*6),null );



--------- CREATING A SIMPLE VIEW -----------------------

-- Create a simple view
CREATE OR REPLACE VIEW <corp_id>_emp_hierarchy_simple_vw
(   employee_id, 
    employee_name,
    employee_job,
    manager_id, 
    department_name,
    department_location,
    salary,
    commission
)
AS 
    (
        SELECT emp.empno, 
             emp.ename,  
             emp.job, 
             TO_NUMBER(emp.mgr) AS manager_id,
             dept.dname,
             dept.loc,
             emp.sal,
             emp.comm
        FROM <corp_id>_emp INNER JOIN <corp_id>_dept
         ON emp.deptno = dept.deptno
        WHERE last_update_dt IS NULL
    );
    
SELECT * FROM    EMPLOYEE_HIERARCHY_SIMPLE_VW ;
--------------------------------------------------------

--------- CREATING A MATERIALIZED VIEW -----------------------

-- MATERIALIZED VIEW
CREATE MATERIALIZED VIEW <corp_id>_emp_hierarchy_mat_vw AS
        SELECT emp.empno, 
             emp.ename,  
             emp.job, 
             TO_NUMBER(emp.mgr) AS manager_id,
             emp.deptno,
             emp.sal,
             emp.comm
        FROM <corp_id>_emp 
        WHERE last_update_dt IS NULL;


select * From <corp_id>_emp_hierarchy_mat_vw;

-------------------------------------------

SHOW VIEWS LIKE '<corp_id>_emp_HIERARCHY_%';

-------------------------------------------------------------------

--------- CREATING A RECURSIVE VIEW WITH BASE-TABLES JOINS -----------------------

-- Recursive VIEW with base tables

CREATE OR REPLACE VIEW <corp_id>_emp_hierarchy_recur_wo_mat_vw
(   employee_id, 
    employee_name,
    employee_job,
    manager_id, 
    manager_name,
    manager_job,
    department_name,
    location,
    salary,
    commission
)
AS 
    (
        WITH RECURSIVE managers 
                -- Column names for the "view"/CTE
                (employee_id, 
                 employee_name,
                 employee_job,
                 manager_id, 
                 manager_name,
                 manager_job,
                 department_id,
                 salary,
                 commission) 
        AS
            -- Common Table Expression
            (
              -- Anchor Clause
              SELECT e.empno AS employee_id, 
                     e.ename AS employee_name, 
                     e.job   AS employee_job, 
                     0  AS manager_id,
                     'NULL'  AS manager_name,
                     'NULL'  AS manager_job,
                     e.deptno AS department_no,
                     e.sal    AS salary,
                     e.comm   AS commission
                FROM <corp_id>_emp e
                WHERE employee_job = 'PRESIDENT'
                 AND last_update_dt IS NULL

                UNION ALL

                -- Recursive Clause
                SELECT emp.empno, 
                     emp.ename,  
                     emp.job, 
                     TO_NUMBER(emp.mgr) AS manager_id,
                     managers.employee_name, 
                     managers.employee_job,
                     emp.deptno,
                     emp.sal,
                     emp.comm
                FROM <corp_id>_emp JOIN managers 
                  ON emp.mgr = managers.employee_id
                WHERE last_update_dt IS NULL
            )
            SELECT  employee_id::int, 
                    employee_name::varchar,
                    employee_job::varchar,
                    manager_id::int, 
                    manager_name::varchar,
                    manager_job::varchar,
                    dname::varchar AS department_name,
                    loc::varchar AS location,
                    salary::decimal,
                    commission::decimal
            FROM managers, <corp_id>_dept
            WHERE dept.deptno = managers.department_id
    );
    
SELECT * FROM <corp_id>_emp_hierarchy_recur_wo_mat_vw ;

-------------------------------------------------------------------

--------- CREATING A RECURSIVE VIEW WITH MATERIALIZED VIEW JOINS -----------------------

-- Recursive VIEW with MATERIALIZED VIEW JOIN

CREATE OR REPLACE VIEW <corp_id>_emp_hierarchy_recur_with_mat_vw
(   employee_id, 
    employee_name,
    employee_job,
    manager_id, 
    manager_name,
    manager_job,
    department_name,
    location,
    salary,
    commission
)
AS 
    (
        WITH RECURSIVE managers 
                -- Column names for the "view"/CTE
                (employee_id, 
                 employee_name,
                 employee_job,
                 manager_id, 
                 manager_name,
                 manager_job,
                 department_id,
                 salary,
                 commission) 
        AS
            -- Common Table Expression
            (
              -- Anchor Clause
              SELECT e.empno AS employee_id, 
                     e.ename AS employee_name, 
                     e.job   AS employee_job, 
                     0  AS manager_id,
                     'NULL'  AS manager_name,
                     'NULL'  AS manager_job,
                     e.deptno AS department_no,
                     e.sal    AS salary,
                     e.comm   AS commission
                FROM <corp_id>_emp_hierarchy_mat_vw e
                WHERE employee_job = 'PRESIDENT'

                UNION ALL

                -- Recursive Clause
                SELECT emp.empno, 
                     emp.ename,  
                     emp.job, 
                     emp.manager_id,
                     managers.employee_name, 
                     managers.employee_job,
                     emp.deptno,
                     emp.sal,
                     emp.comm
                FROM <corp_id>_emp_hierarchy_mat_vw emp JOIN managers 
                  ON emp.manager_id = managers.employee_id
            )
            SELECT  employee_id::int, 
                    employee_name::varchar,
                    employee_job::varchar,
                    manager_id::int, 
                    manager_name::varchar,
                    manager_job::varchar,
                    dname::varchar AS department_name,
                    loc::varchar AS location,
                    salary::decimal,
                    commission::decimal
            FROM managers, <corp_id>_dept
            WHERE dept.deptno = managers.department_id
    );
    
SELECT * FROM <corp_id>_emp_hierarchy_recur_with_mat_vw ;

------------------------------------------------------------------

--------- CREATING A SECURE VIEW -----------------------

CREATE SECURE VIEW <corp_id>_emp_hierarchy_secure_vw AS
        SELECT *
        FROM <corp_id>_emp_hierarchy_recur_wo_mat_vw;

SELECT * FROM <corp_id>_emp_hierarchy_secure_vw ;

--------- CREATING A SECURE MATERIALIZED VIEW -----------------------

CREATE SECURE MATERIALIZED VIEW <corp_id>_emp_hierarchy_secure_mat_vw AS
        SELECT emp.empno, 
             emp.ename,  
             emp.job, 
             TO_NUMBER(emp.mgr) AS manager_id,
             emp.deptno,
             emp.sal,
             emp.comm
        FROM emp 
        WHERE last_update_dt IS NULL;
        
------------------------------------------------------------------

SHOW VIEWS LIKE '<corp_id>_EMP_HIERARCHY_%';

------------------------------------------------------------------