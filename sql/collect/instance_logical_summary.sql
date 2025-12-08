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
