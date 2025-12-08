SET @instance_id := IF(@instance_id IS NULL OR @instance_id=0, /*INSTANCE_ID*/ 0, @instance_id);

SELECT
  NOW() AS stat_time,
  @instance_id AS instance_id,
  t.table_schema AS schema_name,
  t.table_name,
  t.engine,
  t.table_rows,
  t.DATA_LENGTH AS data_bytes,
  t.INDEX_LENGTH AS index_bytes,
  (t.DATA_LENGTH + t.INDEX_LENGTH) AS total_bytes,
  (@rk := @rk + 1) AS rank_no
FROM information_schema.tables t
JOIN (SELECT @rk := 0) r
WHERE t.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY total_bytes DESC
LIMIT 20;
