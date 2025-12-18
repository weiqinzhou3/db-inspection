-- 兼容 MySQL 5.7/8.0，满足 ONLY_FULL_GROUP_BY；被巡检实例与 meta 库均可为 5.7/8.0

-- =========================================================
-- Q1: 巡检失败实例明细（最近一次采集失败）
-- 输出字段：env、alias_name、instance_name、host、port、last_stat_time、logical_total_gb、mysql_version、last_collect_status、error_msg
-- =========================================================
SELECT
  CASE
    WHEN a.env IS NULL OR a.env = '' THEN '-'
    ELSE a.env
  END AS env,
  a.alias_name,
  a.instance_name,
  a.host,
  a.port,
  s.stat_time AS last_stat_time,
  ROUND(s.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  s.mysql_version,
  s.collect_status AS last_collect_status,
  s.error_msg
FROM ops_inspection.snap_mysql_instance_storage s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) ls
  ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND (s.collect_status <> 'ok' OR (s.error_msg IS NOT NULL AND s.error_msg <> ''))
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =========================================================
-- Q2: 按 env 汇总容量（最近一次 vs 上一次）
-- 输出字段：env、instance_count、last_env_total_gb、prev_env_total_gb、diff_env_total_gb、diff_env_total_gb_fmt
-- =========================================================
SELECT
  CASE
    WHEN inst.env_raw IS NULL OR inst.env_raw = '' THEN '-'
    ELSE inst.env_raw
  END AS env,
  COUNT(DISTINCT inst.instance_id) AS instance_count,
  ROUND(SUM(inst.last_total_bytes) / POW(1024, 3), 2) AS last_env_total_gb,
  ROUND(SUM(inst.prev_total_bytes) / POW(1024, 3), 2) AS prev_env_total_gb,
  ROUND(SUM(inst.diff_bytes) / POW(1024, 3), 2) AS diff_env_total_gb,
  CASE
    WHEN SUM(inst.diff_bytes) > 0 THEN CONCAT('+', ROUND(SUM(inst.diff_bytes) / POW(1024, 3), 2))
    WHEN SUM(inst.diff_bytes) < 0 THEN CONCAT('-', ROUND(ABS(SUM(inst.diff_bytes)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_env_total_gb_fmt
FROM (
  SELECT
    a.env AS env_raw,
    a.instance_id,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes,
    (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) AS diff_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    GROUP BY instance_id
  ) last_time
  JOIN ops_inspection.snap_mysql_instance_storage last_rec
    ON last_rec.instance_id = last_time.instance_id AND last_rec.stat_time = last_time.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ops_inspection.snap_mysql_instance_storage s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      GROUP BY instance_id
    ) last2
      ON s.instance_id = last2.instance_id AND s.stat_time < last2.last_time
    GROUP BY s.instance_id
  ) prev_time
    ON prev_time.instance_id = last_time.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = prev_time.instance_id AND prev_rec.stat_time = prev_time.prev_time
  JOIN ops_inspection.asset_instance a
    ON CAST(a.instance_id AS CHAR) = last_time.instance_id
  WHERE a.is_active = 1
) inst
GROUP BY
  CASE
    WHEN inst.env_raw IS NULL OR inst.env_raw = '' THEN '-'
    ELSE inst.env_raw
  END
ORDER BY diff_env_total_gb DESC, last_env_total_gb DESC;

-- =========================================================
-- Q3: 实例容量差异 Top20（按 diff 绝对值排序）
-- 输出字段：env、alias_name、instance_name、host、port、last_stat_time、prev_stat_time、last_total_gb、prev_total_gb、diff_gb、diff_gb_fmt
-- =========================================================
SELECT
  CASE
    WHEN a.env IS NULL OR a.env = '' THEN '-'
    ELSE a.env
  END AS env,
  a.alias_name,
  a.instance_name,
  a.host,
  a.port,
  last_rec.stat_time AS last_stat_time,
  ROUND(last_rec.logical_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  prev_rec.stat_time AS prev_stat_time,
  ROUND(prev_rec.logical_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  ROUND((last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2) AS diff_gb,
  CASE
    WHEN (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) > 0 THEN
      CONCAT('+', ROUND((last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2))
    WHEN (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) < 0 THEN
      CONCAT('-', ROUND(ABS(last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_gb_fmt
FROM (
  SELECT instance_id, MAX(stat_time) AS last_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) last_time
JOIN ops_inspection.snap_mysql_instance_storage last_rec
  ON last_rec.instance_id = last_time.instance_id AND last_rec.stat_time = last_time.last_time
LEFT JOIN (
  SELECT s.instance_id, MAX(s.stat_time) AS prev_time
  FROM ops_inspection.snap_mysql_instance_storage s
  JOIN (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    GROUP BY instance_id
  ) last2
    ON s.instance_id = last2.instance_id AND s.stat_time < last2.last_time
  GROUP BY s.instance_id
) prev_time
  ON prev_time.instance_id = last_time.instance_id
LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
  ON prev_rec.instance_id = prev_time.instance_id AND prev_rec.stat_time = prev_time.prev_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = last_time.instance_id
WHERE a.is_active = 1
ORDER BY ABS(last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) DESC
LIMIT 20;

-- =========================================================
-- Q4: 所有实例最新 vs 上一次容量明细
-- 输出字段：env、alias_name、instance_name、host、port、last_stat_time、prev_stat_time、last_total_gb、prev_total_gb、diff_gb、diff_gb_fmt
-- =========================================================
SELECT
  CASE
    WHEN a.env IS NULL OR a.env = '' THEN '-'
    ELSE a.env
  END AS env,
  a.alias_name,
  a.instance_name,
  a.host,
  a.port,
  last_rec.stat_time AS last_stat_time,
  prev_rec.stat_time AS prev_stat_time,
  ROUND(last_rec.logical_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(prev_rec.logical_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  ROUND((last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2) AS diff_gb,
  CASE
    WHEN (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) > 0 THEN
      CONCAT('+', ROUND((last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2))
    WHEN (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) < 0 THEN
      CONCAT('-', ROUND(ABS(last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_gb_fmt
FROM (
  SELECT instance_id, MAX(stat_time) AS last_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) last_time
JOIN ops_inspection.snap_mysql_instance_storage last_rec
  ON last_rec.instance_id = last_time.instance_id AND last_rec.stat_time = last_time.last_time
LEFT JOIN (
  SELECT s.instance_id, MAX(s.stat_time) AS prev_time
  FROM ops_inspection.snap_mysql_instance_storage s
  JOIN (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    GROUP BY instance_id
  ) last2
    ON s.instance_id = last2.instance_id AND s.stat_time < last2.last_time
  GROUP BY s.instance_id
) prev_time
  ON prev_time.instance_id = last_time.instance_id
LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
  ON prev_rec.instance_id = prev_time.instance_id AND prev_rec.stat_time = prev_time.prev_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = last_time.instance_id
WHERE a.is_active = 1
ORDER BY diff_gb DESC, a.env, a.instance_name;

-- =========================================================
-- Q5: 实例最新容量快照总览
-- 输出字段：env、alias_name、instance_name、host、port、last_stat_time、logical_data_gb、logical_index_gb、logical_total_gb、mysql_version、last_collect_status
-- =========================================================
SELECT
  CASE
    WHEN a.env IS NULL OR a.env = '' THEN '-'
    ELSE a.env
  END AS env,
  a.alias_name,
  a.instance_name,
  a.host,
  a.port,
  s.stat_time AS last_stat_time,
  ROUND(s.logical_data_bytes / POW(1024, 3), 2) AS logical_data_gb,
  ROUND(s.logical_index_bytes / POW(1024, 3), 2) AS logical_index_gb,
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
WHERE a.is_active = 1
ORDER BY s.logical_total_bytes DESC, a.env, a.instance_name;

-- =========================================================
-- Q6: Top20 大表最新 vs 上一次排名变化（每实例）
-- 输出字段：env、alias_name、instance_name、schema_name、table_name、last_total_gb、last_rank_no、prev_total_gb、prev_rank_no、rank_delta、rank_delta_fmt
-- =========================================================
SELECT
  CASE
    WHEN a.env IS NULL OR a.env = '' THEN '-'
    ELSE a.env
  END AS env,
  a.alias_name,
  a.instance_name,
  t_last.schema_name,
  t_last.table_name,
  ROUND(t_last.total_bytes / POW(1024, 3), 2) AS last_total_gb,
  t_last.rank_no AS last_rank_no,
  ROUND(t_prev.total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  t_prev.rank_no AS prev_rank_no,
  (t_prev.rank_no - t_last.rank_no) AS rank_delta,
  CASE
    WHEN t_prev.rank_no IS NULL THEN '-'
    WHEN (t_prev.rank_no - t_last.rank_no) > 0 THEN CONCAT('+', (t_prev.rank_no - t_last.rank_no))
    WHEN (t_prev.rank_no - t_last.rank_no) < 0 THEN CONCAT('-', ABS(t_prev.rank_no - t_last.rank_no))
    ELSE '0'
  END AS rank_delta_fmt
FROM (
  SELECT instance_id, MAX(stat_time) AS last_time
  FROM ops_inspection.snap_mysql_table_topn
  GROUP BY instance_id
) last_round
JOIN ops_inspection.snap_mysql_table_topn t_last
  ON t_last.instance_id = last_round.instance_id AND t_last.stat_time = last_round.last_time
LEFT JOIN (
  SELECT s.instance_id, MAX(s.stat_time) AS prev_time
  FROM ops_inspection.snap_mysql_table_topn s
  JOIN (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_table_topn
    GROUP BY instance_id
  ) last2
    ON s.instance_id = last2.instance_id AND s.stat_time < last2.last_time
  GROUP BY s.instance_id
) prev_round
  ON prev_round.instance_id = last_round.instance_id
LEFT JOIN ops_inspection.snap_mysql_table_topn t_prev
  ON t_prev.instance_id = last_round.instance_id
  AND t_prev.stat_time = prev_round.prev_time
  AND t_prev.schema_name = t_last.schema_name
  AND t_prev.table_name = t_last.table_name
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = t_last.instance_id
WHERE a.is_active = 1
ORDER BY a.env, a.instance_name, t_last.rank_no ASC;
