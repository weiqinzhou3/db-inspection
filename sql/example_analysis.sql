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
FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
  GROUP BY instance_id
) ls ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND s.collect_status = 'failed'
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =============================
-- Q2: 按 env 聚合（仅最新成功实例）
-- =============================
SELECT
  inst_ok.env,
  COUNT(*) AS instance_count,
  ROUND(SUM(inst_ok.last_total_bytes) / POW(1024, 3), 2) AS last_env_total_gb,
  ROUND(IFNULL(SUM(inst_ok.prev_total_bytes), 0) / POW(1024, 3), 2) AS prev_env_total_gb,
  CASE
    WHEN SUM(inst_ok.prev_total_bytes IS NOT NULL) = 0 THEN '-'
    WHEN SUM(inst_ok.last_total_bytes - inst_ok.prev_total_bytes) > 0
      THEN CONCAT('+', ROUND(SUM(inst_ok.last_total_bytes - inst_ok.prev_total_bytes) / POW(1024, 3), 2))
    WHEN SUM(inst_ok.last_total_bytes - inst_ok.prev_total_bytes) < 0
      THEN CONCAT('-', ROUND(ABS(SUM(inst_ok.last_total_bytes - inst_ok.prev_total_bytes)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_env_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    t.instance_id,
    last_rec.logical_total_bytes AS last_total_bytes,
    prev_rec.logical_total_bytes AS prev_total_bytes
  FROM (
    -- 每个实例“最新一条记录”（不区分 OK/failed）
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) AS t
  -- 只保留“最新一条是 OK”的实例
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} AS last_rec
    ON last_rec.instance_id    = t.instance_id
   AND last_rec.stat_time      = t.last_time
   AND last_rec.collect_status = 'ok'
  -- 找“上一次成功”的记录（早于 last_time 且 collect_status='ok'）
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} AS prev_rec
    ON prev_rec.instance_id = t.instance_id
   AND prev_rec.stat_time   = (
         SELECT MAX(s2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
         WHERE s2.instance_id    = t.instance_id
           AND s2.stat_time      < t.last_time
           AND s2.collect_status = 'ok'
       )
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} AS a
    ON a.instance_id = t.instance_id
   AND a.is_active   = 1
   AND a.type        = 'mysql'
) AS inst_ok
GROUP BY inst_ok.env
ORDER BY
  SUM(inst_ok.last_total_bytes - inst_ok.prev_total_bytes) DESC,
  last_env_total_gb DESC;

-- =============================
-- Q3: 实例纬度近两次容量差异
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.host,
  io.port,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes  / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes  / POW(1024, 3), 2) AS prev_data_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_data_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2))
    WHEN io.diff_data_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_index_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2))
    WHEN io.diff_index_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_total_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2))
    WHEN io.diff_total_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    a.host,
    a.port,
    last_rec.stat_time AS last_stat_time,
    prev_rec.stat_time AS prev_stat_time,
    last_rec.logical_data_bytes  AS last_data_bytes,
    prev_rec.logical_data_bytes  AS prev_data_bytes,
    (last_rec.logical_data_bytes  - prev_rec.logical_data_bytes)  AS diff_data_bytes,
    last_rec.logical_index_bytes AS last_index_bytes,
    prev_rec.logical_index_bytes AS prev_index_bytes,
    (last_rec.logical_index_bytes - prev_rec.logical_index_bytes) AS diff_index_bytes,
    last_rec.logical_total_bytes AS last_total_bytes,
    prev_rec.logical_total_bytes AS prev_total_bytes,
    (last_rec.logical_total_bytes - prev_rec.logical_total_bytes) AS diff_total_bytes
  FROM (
    -- 每个实例的“最新一条记录”（不管成功/失败）
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) t
  -- 要求“最新一条记录是 OK”
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} last_rec
    ON last_rec.instance_id = t.instance_id
   AND last_rec.stat_time   = t.last_time
   AND last_rec.collect_status = 'ok'
  -- 找“上一次成功”的记录
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} prev_rec
    ON prev_rec.instance_id = t.instance_id
   AND prev_rec.stat_time   = (
         SELECT MAX(s2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
         WHERE s2.instance_id     = t.instance_id
           AND s2.stat_time       < t.last_time
           AND s2.collect_status  = 'ok'
       )
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON a.instance_id = t.instance_id
   AND a.is_active   = 1
   AND a.type        = 'mysql'
) AS io
ORDER BY
  CASE
    WHEN io.prev_total_bytes IS NULL THEN 0
    ELSE ABS(io.diff_total_bytes)
  END DESC,
  io.env,
  io.instance_name;

-- =============================
-- Q4: （已移除，不再输出）
-- =============================

-- =============================
-- Q5: 表维度当前容量
-- =============================
-- 仅统计“最后一次实例巡检为 OK 的实例”里的大表

SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
  a.alias_name,
  a.instance_name,
  cur.schema_name,
  cur.table_name,
  cur.stat_time    AS last_stat_time,
  cur.table_rows   AS last_table_rows,
  ROUND(cur.data_bytes  / POW(1024, 3), 2) AS last_data_gb,
  ROUND(cur.index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(cur.total_bytes / POW(1024, 3), 2) AS last_total_gb
FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} cur
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
  ON a.instance_id = cur.instance_id
 AND a.is_active   = 1
 AND a.type        = 'mysql'
-- 只保留“最后一次实例巡检为 OK 的实例”
JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s_last
  ON s_last.instance_id = cur.instance_id
 AND s_last.stat_time = (
       SELECT MAX(s2.stat_time)
       FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
       WHERE s2.instance_id = cur.instance_id
     )
 AND s_last.collect_status = 'ok'
-- 当前这条是该表的最新快照
WHERE cur.stat_time = (
        SELECT MAX(c2.stat_time)
        FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} c2
        WHERE c2.instance_id = cur.instance_id
          AND c2.schema_name = cur.schema_name
          AND c2.table_name  = cur.table_name
      )
ORDER BY
  cur.total_bytes DESC,
  env,
  a.alias_name,
  a.instance_name,
  cur.schema_name,
  cur.table_name;

-- =============================
-- Q6: 表维度近两次容量差异（全局差异）
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.schema_name,
  io.table_name,
  io.last_stat_time,
  io.prev_stat_time,
  io.last_table_rows,
  io.prev_table_rows,
  CASE
    WHEN io.prev_table_rows IS NULL THEN '-'
    WHEN (io.last_table_rows - io.prev_table_rows) > 0
      THEN CONCAT('+', (io.last_table_rows - io.prev_table_rows))
    WHEN (io.last_table_rows - io.prev_table_rows) < 0
      THEN CONCAT('-', ABS(io.last_table_rows - io.prev_table_rows))
    ELSE '0'
  END AS diff_table_rows_fmt,
  ROUND(io.last_data_bytes  / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes  / POW(1024, 3), 2) AS prev_data_gb,
  CASE
    WHEN io.prev_data_bytes IS NULL THEN '-'
    WHEN (io.last_data_bytes - io.prev_data_bytes) > 0
      THEN CONCAT('+', ROUND((io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    WHEN (io.last_data_bytes - io.prev_data_bytes) < 0
      THEN CONCAT('-', ROUND(ABS(io.last_data_bytes - io.prev_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE
    WHEN io.prev_index_bytes IS NULL THEN '-'
    WHEN (io.last_index_bytes - io.prev_index_bytes) > 0
      THEN CONCAT('+', ROUND((io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    WHEN (io.last_index_bytes - io.prev_index_bytes) < 0
      THEN CONCAT('-', ROUND(ABS(io.last_index_bytes - io.prev_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_total_bytes IS NULL THEN '-'
    WHEN (io.last_total_bytes - io.prev_total_bytes) > 0
      THEN CONCAT('+', ROUND((io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    WHEN (io.last_total_bytes - io.prev_total_bytes) < 0
      THEN CONCAT('-', ROUND(ABS(io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    cur.instance_id,
    cur.schema_name,
    cur.table_name,
    cur.stat_time    AS last_stat_time,
    cur.table_rows   AS last_table_rows,
    cur.data_bytes   AS last_data_bytes,
    cur.index_bytes  AS last_index_bytes,
    cur.total_bytes  AS last_total_bytes,
    prev.stat_time   AS prev_stat_time,
    prev.table_rows  AS prev_table_rows,
    prev.data_bytes  AS prev_data_bytes,
    prev.index_bytes AS prev_index_bytes,
    prev.total_bytes AS prev_total_bytes
  FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} cur
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON a.instance_id = cur.instance_id
   AND a.is_active   = 1
   AND a.type        = 'mysql'
  -- 只保留“最后一次实例巡检为 OK 的实例”
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s_last
    ON s_last.instance_id = cur.instance_id
   AND s_last.stat_time = (
         SELECT MAX(s2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
         WHERE s2.instance_id = cur.instance_id
       )
   AND s_last.collect_status = 'ok'
  -- 找表的“上一次”快照（小于当前 stat_time 的最大一条）
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} prev
    ON prev.instance_id = cur.instance_id
   AND prev.schema_name = cur.schema_name
   AND prev.table_name  = cur.table_name
   AND prev.stat_time   = (
         SELECT MAX(p2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} p2
         WHERE p2.instance_id = cur.instance_id
           AND p2.schema_name = cur.schema_name
           AND p2.table_name  = cur.table_name
           AND p2.stat_time   < cur.stat_time
       )
  -- 当前这条是该表的最新快照
  WHERE cur.stat_time = (
          SELECT MAX(c2.stat_time)
          FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} c2
          WHERE c2.instance_id = cur.instance_id
            AND c2.schema_name = cur.schema_name
            AND c2.table_name  = cur.table_name
        )
) AS io
ORDER BY io.last_total_bytes DESC, io.env, io.alias_name, io.schema_name, io.table_name;
