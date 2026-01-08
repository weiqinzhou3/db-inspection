-- Preserve pre-set @instance_id if provided by caller
SET @instance_id := IF(@instance_id IS NULL OR @instance_id=0, /*INSTANCE_ID*/ 0, @instance_id);

SELECT
  NOW() AS stat_time,
  @instance_id AS instance_id,
  COALESCE(SUM(DATA_LENGTH), 0) AS logical_data_bytes,
  COALESCE(SUM(INDEX_LENGTH), 0) AS logical_index_bytes,
  COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) AS logical_total_bytes,
  VERSION() AS mysql_version
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');
[root@iZuf6e1gjs4hf6oypkxapeZ sql]# cat collect/top_tables.sql 
SET @instance_id := IF(@instance_id IS NULL OR @instance_id=0, /*INSTANCE_ID*/ 0, @instance_id);

SELECT
  NOW() AS stat_time,
  @instance_id AS instance_id,
  t.table_schema AS schema_name,
  t.table_name,
  t.engine,
  t.table_rows,
  IFNULL(t.DATA_LENGTH, 0) AS data_bytes,
  IFNULL(t.INDEX_LENGTH, 0) AS index_bytes,
  (IFNULL(t.DATA_LENGTH, 0) + IFNULL(t.INDEX_LENGTH, 0)) AS total_bytes,
  (@rk := @rk + 1) AS rank_no
FROM information_schema.tables t
JOIN (SELECT @rk := 0) r
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema NOT IN (
    'mysql',
    'information_schema',
    'performance_schema',
    'sys',
    'mysql_innodb_cluster_metadata'
  )
  AND (
    t.table_rows > 5000000
    OR (IFNULL(t.DATA_LENGTH, 0) + IFNULL(t.INDEX_LENGTH, 0)) > 50 * 1024 * 1024 * 1024
  )
ORDER BY total_bytes DESC;
[root@iZuf6e1gjs4hf6oypkxapeZ sql]# cat load/insert_
insert_instance_storage.sql  insert_table_topn.sql        
[root@iZuf6e1gjs4hf6oypkxapeZ sql]# cat load/insert_instance_storage.sql 
-- Insert template executed on ops_inspection meta database
-- Replace {{...}} placeholders with collected values before execution.
-- Supply string placeholders as quoted literals or NULL when absent.
INSERT INTO ops_inspection.snap_mysql_instance_storage (
  stat_time,
  instance_id,
  logical_data_bytes,
  logical_index_bytes,
  logical_total_bytes,
  mysql_version,
  collect_status,
  error_msg
) VALUES (
  '{{stat_time}}',
  '{{instance_id}}',
  {{logical_data_bytes}},
  {{logical_index_bytes}},
  {{logical_total_bytes}},
  '{{mysql_version}}',
  'ok',
  NULL
);