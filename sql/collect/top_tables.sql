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
