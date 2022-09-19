GRANT CREATE MATERIALIZED VIEW TO admin;
GRANT QUERY REWRITE TO admin;

CREATE MATERIALIZED VIEW emp_mv_cd
BUILD IMMEDIATE 
REFRESH COMPLETE
ON DEMAND
AS
SELECT * FROM emp;

CREATE MATERIALIZED VIEW emp_mv_fad
BUILD IMMEDIATE 
REFRESH FAST
ON DEMAND
AS
SELECT * FROM emp;

CREATE MATERIALIZED VIEW emp_mv_fod
BUILD IMMEDIATE 
REFRESH FORCE
ON DEMAND
AS
SELECT * FROM emp;

CREATE MATERIALIZED VIEW emp_mv_cc
BUILD IMMEDIATE 
REFRESH COMPLETE
ON COMMIT
AS
SELECT * FROM emp;

CREATE MATERIALIZED VIEW emp_mv_fac
BUILD IMMEDIATE 
REFRESH FAST
ON COMMIT
AS
SELECT * FROM emp;

CREATE MATERIALIZED VIEW emp_mv_foc
BUILD IMMEDIATE 
REFRESH FORCE
ON COMMIT
AS
SELECT * FROM emp;

SELECT MVIEW_NAME      , QUERY
     , REWRITE_ENABLED , REFRESH_MODE
     , REFRESH_METHOD  , LAST_REFRESH_DATE
  FROM USER_MVIEWS
 WHERE MVIEW_AME = 'MV_EMP';

 select mview_name, query 
 from user_mviews 
 where mview_name = 'MV_EMP';

SELECT *
FROM user_mviews;

SELECT deptno, SUM(sal)
FROM   emp
GROUP BY deptno;


CREATE MATERIALIZED VIEW emp_mv_fad
BUILD IMMEDIATE 
REFRESH FORCE
ON DEMAND
AS
SELECT * FROM emp

CREATE MATERIALIZED VIEW emp_mv_fd
BUILD IMMEDIATE 
REFRESH FORCE
ON DEMAND
AS
SELECT * FROM emp;

BEGIN
  DBMS_STATS.gather_table_stats(
    ownname => 'admin',
    tabname => 'emp_mv_fd');
END;


CREATE MATERIALIZED VIEW LOG ON admin.emp
TABLESPACE users
WITH PRIMARY KEY
INCLUDING NEW VALUES;

BEGIN
   DBMS_REFRESH.make(
     name                 => 'admin.MINUTE_REFRESH',
     list                 => '',
     next_date            => SYSDATE,
     interval             => '/*1:Mins*/ SYSDATE + 1/(60*24)',
     implicit_destroy     => FALSE,
     lax                  => FALSE,
     job                  => 0,
     rollback_seg         => NULL,
     push_deferred_rpc    => TRUE,
     refresh_after_errors => TRUE,
     purge_option         => NULL,
     parallelism          => NULL,
     heap_size            => NULL);
END;

BEGIN
   DBMS_REFRESH.add(
     name => 'admin.MINUTE_REFRESH',
     list => 'admin.emp_mv_fd',
     lax  => TRUE);
END;

EXEC DBMS_MVIEW.refresh('emp_mv_fd');

SELECT * FROM emp;

UPDATE emp SET SAL =21000 WHERE empno =7876;
COMMIT;

SELECT * FROM emp_mv_fd;


select log_owner,master,log_table,rowids,primary_key
from dba_mview_logs;

SELECT * FROM MLOG$_EMP;

UPDATE emp SET SAL =1800 WHERE empno =7369;

DROP MATERIALIZED VIEW emp_mv_fd;

BEGIN
  DBMS_REFRESH.destroy(name => 'admin.MINUTE_REFRESH');
END;


DROP MATERIALIZED VIEW LOG ON admin.emp;

SET AUTOTRACE TRACE EXPLAIN;

SELECT deptno, SUM(sal)
FROM   emp
GROUP BY deptno;

CREATE MATERIALIZED VIEW emp_aggr_mv
BUILD IMMEDIATE 
REFRESH FORCE
ON DEMAND
ENABLE QUERY REWRITE 
AS
SELECT deptno, SUM(sal) AS sal_by_dept
FROM   emp
GROUP BY deptno;

EXEC DBMS_STATS.gather_table_stats(USER, 'EMP_AGGR_MV');

CREATE TABLE order_lines (
   id            NUMBER(10),
   order_id      NUMBER(10),
   line_qty      NUMBER(5),
   total_value   NUMBER(10,2),
   created_date DATE,
   CONSTRAINT orders_pk PRIMARY KEY (id)
);

INSERT /*+ APPEND */ INTO order_lines
SELECT level AS id,
       TRUNC(DBMS_RANDOM.value(1,1000)) AS order_id,
       TRUNC(DBMS_RANDOM.value(1,20)) AS line_qty,
       ROUND(DBMS_RANDOM.value(1,1000),2) AS total_value,
       TRUNC(SYSDATE - DBMS_RANDOM.value(0,366)) AS created_date
FROM   dual CONNECT BY level <= 100000;
COMMIT;

EXEC DBMS_STATS.gather_table_stats(USER, 'order_lines');

DROP MATERIALIZED VIEW LOG ON order_lines;

CREATE MATERIALIZED VIEW LOG ON order_lines
WITH ROWID, SEQUENCE(order_id, line_qty, total_value)
INCLUDING NEW VALUES;

DROP MATERIALIZED VIEW order_summary_rtmv;

CREATE MATERIALIZED VIEW order_summary_rtmv
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
ENABLE ON QUERY COMPUTATION
AS
SELECT order_id,
       SUM(line_qty) AS sum_line_qty,
       SUM(total_value) AS sum_total_value,
       COUNT(*) AS row_count
FROM   order_lines
GROUP BY order_id;

EXEC DBMS_STATS.gather_table_stats(USER, 'order_summary_rtmv');

SELECT order_id,
       SUM(line_qty) AS sum_line_qty,
       SUM(total_value) AS sum_total_value,
       COUNT(*) AS row_count
FROM   order_lines
WHERE  order_id = 1
GROUP BY order_id;

SET LINESIZE 200 PAGESIZE 100

SELECT *
FROM   dbms_xplan.display_cursor();

INSERT INTO order_lines VALUES (100001, 1, 30, 10000, SYSDATE);
COMMIT;

COLUMN mview_name FORMAT A30

SELECT mview_name,
       staleness,
       on_query_computation
FROM   user_mviews;


SELECT 
  order_id, 
  sum_line_qty, 
  sum_total_value, 
  row_count 
FROM 
  order_summary_rtmv 
WHERE 
  order_id = 1;


SELECT 
  
  /*+ FRESH_MV */
  order_id, 
  sum_line_qty, 
  sum_total_value, 
  row_count 
FROM 
  order_summary_rtmv 
WHERE 
  order_id = 1;'
