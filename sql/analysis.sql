-- 最新容量总览：获取每个实例最新一次快照并关联资产信息
WITH latest_snap AS (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
)
SELECT
  a.instance_id AS asset_instance_id,
  a.instance_name,
  a.alias_name,
  a.env,
  s.stat_time,
  s.logical_data_bytes,
  s.logical_index_bytes,
  s.logical_total_bytes,
  s.mysql_version,
  s.collect_status
FROM latest_snap l
JOIN ops_inspection.snap_mysql_instance_storage s
  ON s.instance_id = l.instance_id AND s.stat_time = l.stat_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = l.instance_id
ORDER BY s.stat_time DESC, a.instance_id;

-- 近 7 天增长：对每个实例比较最近与最早一条快照的容量差
WITH recent_snap AS (
  SELECT *
  FROM ops_inspection.snap_mysql_instance_storage
  WHERE stat_time >= (CURRENT_DATE - INTERVAL 7 DAY)
),
ranked AS (
  SELECT
    instance_id,
    stat_time,
    logical_total_bytes,
    ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY stat_time DESC) AS rn_latest,
    ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY stat_time ASC) AS rn_earliest
  FROM recent_snap
),
latest AS (
  SELECT instance_id, stat_time, logical_total_bytes
  FROM ranked
  WHERE rn_latest = 1
),
earliest AS (
  SELECT instance_id, stat_time, logical_total_bytes
  FROM ranked
  WHERE rn_earliest = 1
)
SELECT
  CAST(a.instance_id AS CHAR) AS instance_id,
  a.instance_name,
  a.env,
  l.stat_time AS latest_stat_time,
  e.stat_time AS earliest_stat_time,
  l.logical_total_bytes AS latest_total_bytes,
  e.logical_total_bytes AS earliest_total_bytes,
  (l.logical_total_bytes - e.logical_total_bytes) AS growth_bytes
FROM latest l
JOIN earliest e ON l.instance_id = e.instance_id
LEFT JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = l.instance_id
ORDER BY growth_bytes DESC;

-- 大表增长 TopN：比较近 7 天内同一表的最新与最早快照增量
WITH recent_topn AS (
  SELECT *
  FROM ops_inspection.snap_mysql_table_topn
  WHERE stat_time >= (CURRENT_DATE - INTERVAL 7 DAY)
),
ranked AS (
  SELECT
    instance_id,
    schema_name,
    table_name,
    stat_time,
    total_bytes,
    ROW_NUMBER() OVER (
      PARTITION BY instance_id, schema_name, table_name
      ORDER BY stat_time DESC
    ) AS rn_latest,
    ROW_NUMBER() OVER (
      PARTITION BY instance_id, schema_name, table_name
      ORDER BY stat_time ASC
    ) AS rn_earliest
  FROM recent_topn
),
latest AS (
  SELECT instance_id, schema_name, table_name, stat_time, total_bytes
  FROM ranked WHERE rn_latest = 1
),
earliest AS (
  SELECT instance_id, schema_name, table_name, stat_time, total_bytes
  FROM ranked WHERE rn_earliest = 1
)
SELECT
  l.instance_id,
  l.schema_name,
  l.table_name,
  l.stat_time AS latest_stat_time,
  e.stat_time AS earliest_stat_time,
  l.total_bytes AS latest_total_bytes,
  e.total_bytes AS earliest_total_bytes,
  (l.total_bytes - e.total_bytes) AS growth_bytes
FROM latest l
LEFT JOIN earliest e
  ON l.instance_id = e.instance_id
  AND l.schema_name = e.schema_name
  AND l.table_name = e.table_name
ORDER BY growth_bytes DESC
LIMIT 20;
