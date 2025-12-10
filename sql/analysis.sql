-- =========================================================
-- Q1: 实例最新逻辑大小总览（按 env / instance_name 展示）
-- 输出字段：env、instance_name、alias_name、host、port、last_stat_time、logical_*_bytes、logical_total_gb、mysql_version、last_collect_status
-- =========================================================
WITH latest_instance AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (PARTITION BY s.instance_id ORDER BY s.stat_time DESC) AS rn
  FROM ops_inspection.snap_mysql_instance_storage s
)
SELECT
  a.env,
  a.instance_name,
  a.alias_name,
  a.host,
  a.port,
  l.stat_time AS last_stat_time,
  l.logical_data_bytes,
  l.logical_index_bytes,
  l.logical_total_bytes,
  ROUND(l.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  l.mysql_version,
  l.collect_status AS last_collect_status
FROM latest_instance l
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = l.instance_id
WHERE l.rn = 1
ORDER BY l.logical_total_bytes DESC, a.env, a.instance_name;

-- =========================================================
-- Q2: 实例逻辑容量增长（最近 10 条 snapshot 对比）
-- 输出字段：env、instance_name、host、port、first_time/last_time、first_total_gb/last_total_gb、diff_gb、avg_growth_gb_per_day
-- 说明：若仅有 1 条数据，diff 与日均增长视为 0
-- =========================================================
WITH ranked AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (PARTITION BY s.instance_id ORDER BY s.stat_time DESC) AS rn_desc
  FROM ops_inspection.snap_mysql_instance_storage s
),
recent AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (PARTITION BY r.instance_id ORDER BY r.stat_time ASC) AS rn_asc
  FROM ranked r
  WHERE r.rn_desc <= 10
),
agg AS (
  SELECT
    instance_id,
    MAX(CASE WHEN rn_desc = 1 THEN stat_time END) AS last_time,
    MAX(CASE WHEN rn_desc = 1 THEN logical_total_bytes END) AS last_total_bytes,
    MAX(CASE WHEN rn_asc = 1 THEN stat_time END) AS first_time,
    MAX(CASE WHEN rn_asc = 1 THEN logical_total_bytes END) AS first_total_bytes
  FROM recent
  GROUP BY instance_id
)
SELECT
  a.env,
  a.instance_name,
  a.host,
  a.port,
  ag.first_time,
  ag.last_time,
  ROUND(ag.first_total_bytes / POW(1024, 3), 2) AS first_total_gb,
  ROUND(ag.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND((ag.last_total_bytes - ag.first_total_bytes) / POW(1024, 3), 2) AS diff_gb,
  ROUND(
    CASE
      WHEN TIMESTAMPDIFF(SECOND, ag.first_time, ag.last_time) > 0 THEN
        (ag.last_total_bytes - ag.first_total_bytes) / (TIMESTAMPDIFF(SECOND, ag.first_time, ag.last_time) / 86400)
      ELSE 0
    END / POW(1024, 3),
    2
  ) AS avg_growth_gb_per_day
FROM agg ag
LEFT JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = ag.instance_id
ORDER BY avg_growth_gb_per_day DESC, ag.last_time DESC;

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
WITH ranked_topn AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY t.instance_id, t.schema_name, t.table_name
      ORDER BY t.stat_time DESC
    ) AS rn
  FROM ops_inspection.snap_mysql_table_topn t
)
SELECT
  a.env,
  a.instance_name,
  a.host,
  a.port,
  rt.stat_time,
  rt.schema_name,
  rt.table_name,
  rt.engine,
  rt.table_rows,
  rt.data_bytes,
  rt.index_bytes,
  rt.total_bytes,
  ROUND(rt.total_bytes / POW(1024, 3), 2) AS total_gb,
  rt.rank_no
FROM ranked_topn rt
LEFT JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = rt.instance_id
WHERE rt.rn = 1
ORDER BY rt.total_bytes DESC, rt.rank_no ASC;

-- =========================================================
-- Q5: 按环境汇总实例总逻辑容量（便于看整体占用）
-- 输出字段：env、instance_count、total_bytes、total_gb
-- =========================================================
WITH latest_env AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (PARTITION BY s.instance_id ORDER BY s.stat_time DESC) AS rn
  FROM ops_inspection.snap_mysql_instance_storage s
)
SELECT
  a.env,
  COUNT(DISTINCT a.instance_id) AS instance_count,
  SUM(l.logical_total_bytes) AS total_bytes,
  ROUND(SUM(l.logical_total_bytes) / POW(1024, 3), 2) AS total_gb
FROM latest_env l
JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = l.instance_id
WHERE l.rn = 1
GROUP BY a.env
ORDER BY total_bytes DESC;
