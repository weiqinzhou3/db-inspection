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
