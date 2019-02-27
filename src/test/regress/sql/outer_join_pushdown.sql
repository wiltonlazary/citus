-- ===================================================================
-- test complex outer join trees 
-- ===================================================================

CREATE SCHEMA outer_join_pushdown;
SET search_path TO outer_join_pushdown;

CREATE TABLE distributed_1 (col1 int, col2 int, distrib_col int);
CREATE TABLE distributed_2 (col1 int, col2 int, distrib_col int);
CREATE TABLE reference_1 (col1 int, col2 int);
CREATE TABLE reference_2(col1 int, col2 int);

SELECT create_distributed_table('distributed_1','distrib_col');
SELECT create_distributed_table('distributed_2','distrib_col');
SELECT create_reference_table('reference_1');
SELECT create_reference_table('reference_2');

INSERT INTO distributed_1 SELECT i, i, i FROM generate_series(0,100) i;
INSERT INTO distributed_2 SELECT i%2, i%2, i%2 FROM generate_series(0,100) i;
INSERT INTO reference_1 SELECT i%3, i%3 FROM generate_series(0,100) i;
INSERT INTO reference_2 SELECT i%4, i%4 FROM generate_series(0,100) i;

select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join (select distrib_col,count(*) from distributed_2 group by distrib_col) d2 ON d2.distrib_col=d1.distrib_col;

with d2 AS (select distrib_col,count(*) from distributed_2 group by distrib_col)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join d2 ON d2.distrib_col=d1.distrib_col;

with d2 AS (select distrib_col,col1 from distributed_2)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join d2 ON d2.distrib_col=d1.distrib_col;

with cte_1 AS (select col1 from reference_1)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join cte_1 ON cte_1.col1=d1.distrib_col;

RESET search_path;
DROP SCHEMA outer_join_pushdown CASCADE;
