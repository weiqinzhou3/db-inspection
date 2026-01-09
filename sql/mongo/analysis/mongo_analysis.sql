-- MongoDB inspection analysis (MySQL 5.7/8.0, ONLY_FULL_GROUP_BY friendly)

-- =============================
-- Q1: Latest instance snapshot
-- =============================
SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
  a.alias_name,
  a.instance_name,
  s.stat_time AS last_stat_time,
  ROUND(s.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  ROUND(s.physical_total_bytes / POW(1024, 3), 2) AS physical_total_gb,
  s.mongo_version,
  s.collect_status AS last_collect_status,
  s.error_msg
FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE}
  GROUP BY instance_id
) ls ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a ON a.instance_id = s.instance_id
WHERE a.is_active = 1
  AND a.type = 'mongo'
  AND a.auth_mode = 'mongo_uri_aes'
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =============================
-- Q2: Env summary (latest per instance)
-- =============================
SELECT
  inst.env,
  COUNT(*) AS instance_count,
  ROUND(SUM(inst.last_logical_bytes) / POW(1024, 3), 2) AS last_env_logical_total_gb,
  ROUND(SUM(inst.last_physical_bytes) / POW(1024, 3), 2) AS last_env_physical_total_gb
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    s.instance_id,
    s.logical_total_bytes AS last_logical_bytes,
    s.physical_total_bytes AS last_physical_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS stat_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) t
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} s
    ON s.instance_id = t.instance_id
   AND s.stat_time = t.stat_time
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON a.instance_id = t.instance_id
   AND a.is_active = 1
   AND a.type = 'mongo'
   AND a.auth_mode = 'mongo_uri_aes'
) inst
GROUP BY inst.env
ORDER BY last_env_logical_total_gb DESC, inst.env;

-- =============================
-- Q3: Instance last vs prev (OK only)
-- =============================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.host,
  io.port,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_logical_bytes / POW(1024, 3), 2) AS last_logical_total_gb,
  ROUND(io.prev_logical_bytes / POW(1024, 3), 2) AS prev_logical_total_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_logical_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_logical_bytes / POW(1024, 3), 2))
    WHEN io.diff_logical_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_logical_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_logical_total_gb_fmt,
  ROUND(io.last_physical_bytes / POW(1024, 3), 2) AS last_physical_total_gb,
  ROUND(io.prev_physical_bytes / POW(1024, 3), 2) AS prev_physical_total_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_physical_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_physical_bytes / POW(1024, 3), 2))
    WHEN io.diff_physical_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_physical_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_physical_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    a.host,
    a.port,
    last_rec.stat_time AS last_stat_time,
    prev_rec.stat_time AS prev_stat_time,
    last_rec.logical_total_bytes AS last_logical_bytes,
    prev_rec.logical_total_bytes AS prev_logical_bytes,
    (last_rec.logical_total_bytes - prev_rec.logical_total_bytes) AS diff_logical_bytes,
    last_rec.physical_total_bytes AS last_physical_bytes,
    prev_rec.physical_total_bytes AS prev_physical_bytes,
    (last_rec.physical_total_bytes - prev_rec.physical_total_bytes) AS diff_physical_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) t
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} last_rec
    ON last_rec.instance_id = t.instance_id
   AND last_rec.stat_time = t.last_time
   AND last_rec.collect_status = 'ok'
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} prev_rec
    ON prev_rec.instance_id = t.instance_id
   AND prev_rec.stat_time = (
         SELECT MAX(s2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} s2
         WHERE s2.instance_id = t.instance_id
           AND s2.stat_time < t.last_time
           AND s2.collect_status = 'ok'
       )
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON a.instance_id = t.instance_id
   AND a.is_active = 1
   AND a.type = 'mongo'
   AND a.auth_mode = 'mongo_uri_aes'
) AS io
ORDER BY
  last_logical_total_gb DESC,
  io.env,
  io.instance_name;

-- =============================
-- Q4: Collection TopN (latest)
-- =============================
SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
  a.alias_name,
  a.instance_name,
  cur.db_name,
  cur.coll_name,
  cur.doc_count,
  ROUND(cur.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  ROUND(cur.physical_total_bytes / POW(1024, 3), 2) AS physical_total_gb
FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_COLLECTION_TOPN} cur
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
  ON a.instance_id = cur.instance_id
 AND a.is_active = 1
 AND a.type = 'mongo'
 AND a.auth_mode = 'mongo_uri_aes'
WHERE cur.stat_time = (
  SELECT MAX(c2.stat_time)
  FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_COLLECTION_TOPN} c2
  WHERE c2.instance_id = cur.instance_id
    AND c2.db_name = cur.db_name
    AND c2.coll_name = cur.coll_name
)
ORDER BY logical_total_gb DESC
LIMIT 50;
