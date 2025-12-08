-- Instance logical capacity snapshot (exclude system schemas)
-- Bind :instance_id from ops_inspection.asset_instance.instance_id for the target MySQL instance.
INSERT INTO ops_inspection.snap_mysql_instance_storage (
  stat_time,
  instance_id,
  logical_data_bytes,
  logical_index_bytes,
  logical_total_bytes,
  mysql_version,
  collect_status,
  error_msg
)
SELECT
  NOW() AS stat_time,
  :instance_id AS instance_id,
  COALESCE(SUM(DATA_LENGTH), 0) AS logical_data_bytes,
  COALESCE(SUM(INDEX_LENGTH), 0) AS logical_index_bytes,
  COALESCE(SUM(DATA_LENGTH + INDEX_LENGTH), 0) AS logical_total_bytes,
  VERSION() AS mysql_version,
  'ok' AS collect_status,
  NULL AS error_msg
FROM information_schema.tables
WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');

-- Top20 largest tables by total bytes for the instance
WITH ranked_tables AS (
  SELECT
    NOW() AS stat_time,
    :instance_id AS instance_id,
    table_schema AS schema_name,
    table_name,
    engine,
    table_rows,
    DATA_LENGTH AS data_bytes,
    INDEX_LENGTH AS index_bytes,
    (DATA_LENGTH + INDEX_LENGTH) AS total_bytes,
    ROW_NUMBER() OVER (ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC) AS rank_no
  FROM information_schema.tables
  WHERE table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
)
INSERT INTO ops_inspection.snap_mysql_table_topn (
  stat_time,
  instance_id,
  schema_name,
  table_name,
  engine,
  table_rows,
  data_bytes,
  index_bytes,
  total_bytes,
  rank_no
)
SELECT
  stat_time,
  instance_id,
  schema_name,
  table_name,
  engine,
  table_rows,
  data_bytes,
  index_bytes,
  total_bytes,
  rank_no
FROM ranked_tables
WHERE rank_no <= 20
ORDER BY rank_no;
