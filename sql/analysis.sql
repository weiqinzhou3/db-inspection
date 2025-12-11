-- 兼容 MySQL 5.7/8，meta 库 ops_inspection 也可运行在 5.7/8

-- =========================================================
-- Q1: 实例最新逻辑大小总览（按 env / instance_name 展示）
-- 输出字段：env、instance_name、alias_name、host、port、last_stat_time、logical_*_bytes、logical_total_gb、mysql_version、last_collect_status
-- =========================================================
SELECT
  a.env,
  a.instance_name,
  a.alias_name,
  a.host,
  a.port,
  s.stat_time AS last_stat_time,
  s.logical_data_bytes,
  s.logical_index_bytes,
  s.logical_total_bytes,
  ROUND(s.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  s.mysql_version,
  s.collect_status AS last_collect_status
FROM ops_inspection.snap_mysql_instance_storage s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) ls
  ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = s.instance_id
ORDER BY s.logical_total_bytes DESC, a.env, a.instance_name;

-- =========================================================
-- Q2: 实例逻辑容量增长（最近 10 条 snapshot 对比）
-- 输出字段：env、instance_name、host、port、first_time/last_time、first_total_gb/last_total_gb、diff_gb、avg_growth_gb_per_day
-- 说明：若仅有 1 条数据，diff 与日均增长视为 0
-- =========================================================
SELECT
  a.env,
  a.instance_name,
  a.host,
  a.port,
  summary.first_time,
  summary.last_time,
  ROUND(first_rec.logical_total_bytes / POW(1024, 3), 2) AS first_total_gb,
  ROUND(last_rec.logical_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND((last_rec.logical_total_bytes - first_rec.logical_total_bytes) / POW(1024, 3), 2) AS diff_gb,
  ROUND(
    CASE
      WHEN TIMESTAMPDIFF(SECOND, summary.first_time, summary.last_time) > 0 THEN
        (last_rec.logical_total_bytes - first_rec.logical_total_bytes) / (TIMESTAMPDIFF(SECOND, summary.first_time, summary.last_time) / 86400)
      ELSE 0
    END / POW(1024, 3),
    2
  ) AS avg_growth_gb_per_day
FROM (
  SELECT r.instance_id,
         MIN(r.stat_time) AS first_time,
         MAX(r.stat_time) AS last_time
  FROM (
    SELECT s.*
    FROM ops_inspection.snap_mysql_instance_storage s
    WHERE (
      SELECT COUNT(*) FROM ops_inspection.snap_mysql_instance_storage s2
      WHERE s2.instance_id = s.instance_id AND s2.stat_time > s.stat_time
    ) < 10
  ) r
  GROUP BY r.instance_id
) summary
JOIN (
  SELECT s.*
  FROM ops_inspection.snap_mysql_instance_storage s
  WHERE (
    SELECT COUNT(*) FROM ops_inspection.snap_mysql_instance_storage s2
    WHERE s2.instance_id = s.instance_id AND s2.stat_time > s.stat_time
  ) < 10
) recent ON recent.instance_id = summary.instance_id
JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = summary.instance_id
JOIN ops_inspection.snap_mysql_instance_storage first_rec
  ON first_rec.instance_id = summary.instance_id AND first_rec.stat_time = summary.first_time
JOIN ops_inspection.snap_mysql_instance_storage last_rec
  ON last_rec.instance_id = summary.instance_id AND last_rec.stat_time = summary.last_time
GROUP BY summary.instance_id
ORDER BY avg_growth_gb_per_day DESC, summary.last_time DESC;

-- =========================================================
-- Q3: 巡检失败实例明细（可按时间过滤）
-- 输出字段：stat_time、env、instance_name、host、port、collect_status、error_msg
-- 可按需调整 WHERE 条件过滤时间范围
-- =========================================================
SELECT
  s.stat_time,
  a.env,
  a.instance_name,
  a.host,
  a.port,
  s.collect_status,
  s.error_msg
FROM ops_inspection.snap_mysql_instance_storage s
LEFT JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE s.collect_status <> 'ok'
  AND s.stat_time >= (CURRENT_DATE - INTERVAL 7 DAY)
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =========================================================
-- Q4: 最新 Top20 表空间明细（按实例+schema+table 最新一批）
-- 输出字段：env、instance_name、host、port、stat_time、schema_name、table_name、engine、table_rows、data_bytes、index_bytes、total_bytes、total_gb、rank_no
-- 可在 WHERE 中追加 env / instance_name 过滤
-- =========================================================
SELECT
  a.env,
  a.instance_name,
  a.host,
  a.port,
  t.stat_time,
  t.schema_name,
  t.table_name,
  t.engine,
  t.table_rows,
  t.data_bytes,
  t.index_bytes,
  t.total_bytes,
  ROUND(t.total_bytes / POW(1024, 3), 2) AS total_gb,
  t.rank_no
FROM ops_inspection.snap_mysql_table_topn t
JOIN (
  SELECT instance_id, schema_name, table_name, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_table_topn
  GROUP BY instance_id, schema_name, table_name
) lt
  ON t.instance_id = lt.instance_id
  AND t.schema_name = lt.schema_name
  AND t.table_name = lt.table_name
  AND t.stat_time = lt.stat_time
LEFT JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = t.instance_id
ORDER BY t.total_bytes DESC, t.rank_no ASC;

-- =========================================================
-- Q5: 按环境汇总实例总逻辑容量（便于看整体占用）
-- 输出字段：env、instance_count、total_bytes、total_gb
-- =========================================================
SELECT
  a.env,
  COUNT(DISTINCT a.instance_id) AS instance_count,
  SUM(s.logical_total_bytes) AS total_bytes,
  ROUND(SUM(s.logical_total_bytes) / POW(1024, 3), 2) AS total_gb
FROM ops_inspection.snap_mysql_instance_storage s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) latest ON s.instance_id = latest.instance_id AND s.stat_time = latest.stat_time
JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = s.instance_id
GROUP BY a.env
ORDER BY total_bytes DESC;
