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
