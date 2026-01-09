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
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) AS t
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} AS last_rec
    ON last_rec.instance_id    = t.instance_id
   AND last_rec.stat_time      = t.last_time
   AND last_rec.collect_status = 'ok'
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
