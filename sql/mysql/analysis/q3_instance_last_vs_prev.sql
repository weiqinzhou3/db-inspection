SELECT
  io.env,
  io.alias_name,
  io.instance_name,
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
  END AS diff_total_gb_fmt,
  io.last_stat_time,
  io.prev_stat_time
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
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) t
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} last_rec
    ON last_rec.instance_id = t.instance_id
   AND last_rec.stat_time   = t.last_time
   AND last_rec.collect_status = 'ok'
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
  last_total_gb DESC,
  io.env,
  io.instance_name;
