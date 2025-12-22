-- 兼容 MySQL 5.7/8.0，满足 ONLY_FULL_GROUP_BY；被巡检实例与 meta 库均可为 5.7/8.0

-- =============================
-- Q1: 最新失败实例
-- =============================
SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
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
) ls ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND s.collect_status = 'failed'
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =============================
-- Q2: 按 env 聚合（仅最新成功实例）
-- =============================
SELECT
  env,
  COUNT(*) AS instance_count,
  ROUND(SUM(last_total_bytes) / POW(1024, 3), 2) AS last_env_total_gb,
  ROUND(SUM(prev_total_bytes) / POW(1024, 3), 2) AS prev_env_total_gb,
  CASE
    WHEN SUM(last_total_bytes - prev_total_bytes) > 0 THEN CONCAT('+', ROUND(SUM(last_total_bytes - prev_total_bytes) / POW(1024, 3), 2))
    WHEN SUM(last_total_bytes - prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(SUM(last_total_bytes - prev_total_bytes)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_env_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    last_rec.instance_id,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    WHERE collect_status = 'ok'
    GROUP BY instance_id
  ) lt
  JOIN ops_inspection.snap_mysql_instance_storage last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ops_inspection.snap_mysql_instance_storage s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time AND s.collect_status = 'ok'
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
) inst_ok
GROUP BY env
ORDER BY SUM(last_total_bytes - prev_total_bytes) DESC, last_env_total_gb DESC;

-- =============================
-- Q3: 实例最近 vs 上一次容量（成功实例）
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.host,
  io.port,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_data_bytes > 0 THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2)) WHEN io.diff_data_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_index_bytes > 0 THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2)) WHEN io.diff_index_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_total_bytes > 0 THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2)) WHEN io.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    a.host,
    a.port,
    last_rec.stat_time AS last_stat_time,
    prev_rec.stat_time AS prev_stat_time,
    last_rec.logical_data_bytes AS last_data_bytes,
    IFNULL(prev_rec.logical_data_bytes, 0) AS prev_data_bytes,
    (last_rec.logical_data_bytes - IFNULL(prev_rec.logical_data_bytes, 0)) AS diff_data_bytes,
    last_rec.logical_index_bytes AS last_index_bytes,
    IFNULL(prev_rec.logical_index_bytes, 0) AS prev_index_bytes,
    (last_rec.logical_index_bytes - IFNULL(prev_rec.logical_index_bytes, 0)) AS diff_index_bytes,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes,
    (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) AS diff_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    WHERE collect_status = 'ok'
    GROUP BY instance_id
  ) lt
  JOIN ops_inspection.snap_mysql_instance_storage last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ops_inspection.snap_mysql_instance_storage s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time AND s.collect_status = 'ok'
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
) io
ORDER BY ABS(io.diff_total_bytes) DESC, io.env, io.instance_name;

-- =============================
-- Q4: （已移除，不再输出）
-- =============================

-- =============================
-- Q5: 表维度当前容量 Top10（全局 Top10）
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.schema_name,
  io.table_name,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_data_bytes IS NULL THEN '-'
    WHEN (io.last_data_bytes - io.prev_data_bytes) > 0 THEN CONCAT('+', ROUND((io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    WHEN (io.last_data_bytes - io.prev_data_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  CASE
    WHEN io.prev_index_bytes IS NULL THEN '-'
    WHEN (io.last_index_bytes - io.prev_index_bytes) > 0 THEN CONCAT('+', ROUND((io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    WHEN (io.last_index_bytes - io.prev_index_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  CASE
    WHEN io.prev_total_bytes IS NULL THEN '-'
    WHEN (io.last_total_bytes - io.prev_total_bytes) > 0 THEN CONCAT('+', ROUND((io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    WHEN (io.last_total_bytes - io.prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    inst.env,
    inst.alias_name,
    inst.instance_name,
    inst.instance_id,
    inst.last_time AS last_stat_time,
    inst.prev_time AS prev_stat_time,
    last_tbl.schema_name,
    last_tbl.table_name,
    last_tbl.data_bytes AS last_data_bytes,
    last_tbl.index_bytes AS last_index_bytes,
    last_tbl.total_bytes AS last_total_bytes,
    prev_tbl.data_bytes AS prev_data_bytes,
    prev_tbl.index_bytes AS prev_index_bytes,
    prev_tbl.total_bytes AS prev_total_bytes
  FROM (
    SELECT
      CAST(a.instance_id AS CHAR) AS instance_id,
      CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
      a.alias_name,
      a.instance_name,
      lt.last_time,
      pt.prev_time
    FROM (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) lt
    LEFT JOIN (
      SELECT
        s.instance_id,
        MAX(s.stat_time) AS prev_time
      FROM ops_inspection.snap_mysql_instance_storage s
      JOIN (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_instance_storage
        WHERE collect_status = 'ok'
        GROUP BY instance_id
      ) l2 ON l2.instance_id = s.instance_id
        AND s.stat_time < l2.last_time
        AND s.collect_status = 'ok'
      GROUP BY s.instance_id
    ) pt ON pt.instance_id = lt.instance_id
    JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
    WHERE a.is_active = 1
      AND a.type = 'mysql'
  ) inst
  JOIN (
    SELECT
      instance_id,
      schema_name,
      table_name,
      stat_time,
      SUM(data_bytes) AS data_bytes,
      SUM(index_bytes) AS index_bytes,
      SUM(total_bytes) AS total_bytes
    FROM ops_inspection.snap_mysql_table_topn
    GROUP BY instance_id, schema_name, table_name, stat_time
  ) last_tbl ON last_tbl.instance_id = inst.instance_id
    AND last_tbl.stat_time = inst.last_time
  LEFT JOIN (
    SELECT
      instance_id,
      schema_name,
      table_name,
      stat_time,
      SUM(data_bytes) AS data_bytes,
      SUM(index_bytes) AS index_bytes,
      SUM(total_bytes) AS total_bytes
    FROM ops_inspection.snap_mysql_table_topn
    GROUP BY instance_id, schema_name, table_name, stat_time
  ) prev_tbl ON prev_tbl.instance_id = inst.instance_id
    AND prev_tbl.schema_name = last_tbl.schema_name
    AND prev_tbl.table_name = last_tbl.table_name
    AND prev_tbl.stat_time = inst.prev_time
) io
ORDER BY io.last_total_bytes DESC, io.env, io.alias_name, io.schema_name, io.table_name
LIMIT 10;
-- =============================
-- Q6: 表维度近两次容量差异 Top10（全局差异）
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.schema_name,
  io.table_name,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_data_bytes IS NULL THEN '-'
    WHEN (io.last_data_bytes - io.prev_data_bytes) > 0 THEN CONCAT('+', ROUND((io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    WHEN (io.last_data_bytes - io.prev_data_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  CASE
    WHEN io.prev_index_bytes IS NULL THEN '-'
    WHEN (io.last_index_bytes - io.prev_index_bytes) > 0 THEN CONCAT('+', ROUND((io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    WHEN (io.last_index_bytes - io.prev_index_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  CASE
    WHEN io.prev_total_bytes IS NULL THEN '-'
    WHEN (io.last_total_bytes - io.prev_total_bytes) > 0 THEN CONCAT('+', ROUND((io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    WHEN (io.last_total_bytes - io.prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    inst.env,
    inst.alias_name,
    inst.instance_name,
    inst.instance_id,
    inst.last_time AS last_stat_time,
    inst.prev_time AS prev_stat_time,
    last_tbl.schema_name,
    last_tbl.table_name,
    last_tbl.data_bytes AS last_data_bytes,
    last_tbl.index_bytes AS last_index_bytes,
    last_tbl.total_bytes AS last_total_bytes,
    prev_tbl.data_bytes AS prev_data_bytes,
    prev_tbl.index_bytes AS prev_index_bytes,
    prev_tbl.total_bytes AS prev_total_bytes
  FROM (
    SELECT
      CAST(a.instance_id AS CHAR) AS instance_id,
      CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
      a.alias_name,
      a.instance_name,
      lt.last_time,
      pt.prev_time
    FROM (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) lt
    LEFT JOIN (
      SELECT
        s.instance_id,
        MAX(s.stat_time) AS prev_time
      FROM ops_inspection.snap_mysql_instance_storage s
      JOIN (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_instance_storage
        WHERE collect_status = 'ok'
        GROUP BY instance_id
      ) l2 ON l2.instance_id = s.instance_id
        AND s.stat_time < l2.last_time
        AND s.collect_status = 'ok'
      GROUP BY s.instance_id
    ) pt ON pt.instance_id = lt.instance_id
    JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
    WHERE a.is_active = 1
      AND a.type = 'mysql'
  ) inst
  JOIN (
    SELECT
      instance_id,
      schema_name,
      table_name,
      stat_time,
      SUM(data_bytes) AS data_bytes,
      SUM(index_bytes) AS index_bytes,
      SUM(total_bytes) AS total_bytes
    FROM ops_inspection.snap_mysql_table_topn
    GROUP BY instance_id, schema_name, table_name, stat_time
  ) last_tbl ON last_tbl.instance_id = inst.instance_id
    AND last_tbl.stat_time = inst.last_time
  LEFT JOIN (
    SELECT
      instance_id,
      schema_name,
      table_name,
      stat_time,
      SUM(data_bytes) AS data_bytes,
      SUM(index_bytes) AS index_bytes,
      SUM(total_bytes) AS total_bytes
    FROM ops_inspection.snap_mysql_table_topn
    GROUP BY instance_id, schema_name, table_name, stat_time
  ) prev_tbl ON prev_tbl.instance_id = inst.instance_id
    AND prev_tbl.schema_name = last_tbl.schema_name
    AND prev_tbl.table_name = last_tbl.table_name
    AND prev_tbl.stat_time = inst.prev_time
) io
ORDER BY ABS(io.last_total_bytes - io.prev_total_bytes) DESC,
  io.env,
  io.alias_name,
  io.instance_name,
  io.schema_name,
  io.table_name
LIMIT 10;
