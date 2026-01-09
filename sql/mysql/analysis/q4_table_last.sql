SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
  a.alias_name,
  a.instance_name,
  cur.schema_name,
  cur.table_name,
  cur.table_rows   AS last_table_rows,
  ROUND(cur.data_bytes  / POW(1024, 3), 2) AS last_data_gb,
  ROUND(cur.index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(cur.total_bytes / POW(1024, 3), 2) AS last_total_gb,
  cur.stat_time    AS last_stat_time
FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} cur
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
  ON a.instance_id = cur.instance_id
 AND a.is_active   = 1
 AND a.type        = 'mysql'
JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s_last
  ON s_last.instance_id = cur.instance_id
 AND s_last.stat_time = (
       SELECT MAX(s2.stat_time)
       FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
       WHERE s2.instance_id = cur.instance_id
     )
 AND s_last.collect_status = 'ok'
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
