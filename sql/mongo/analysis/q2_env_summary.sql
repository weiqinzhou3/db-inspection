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
