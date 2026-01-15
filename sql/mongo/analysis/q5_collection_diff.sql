SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.db_name,
  io.coll_name,
  io.last_stat_time,
  io.prev_stat_time,
  io.last_doc_count,
  io.prev_doc_count,
  CASE
    WHEN io.prev_doc_count IS NULL THEN '-'
    WHEN io.diff_doc_count > 0
      THEN CONCAT('+', io.diff_doc_count)
    WHEN io.diff_doc_count < 0
      THEN CONCAT('-', ABS(io.diff_doc_count))
    ELSE '0'
  END AS diff_doc_count_fmt,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  CASE
    WHEN io.prev_doc_count IS NULL THEN '-'
    WHEN io.diff_data_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2))
    WHEN io.diff_data_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE
    WHEN io.prev_doc_count IS NULL THEN '-'
    WHEN io.diff_index_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2))
    WHEN io.diff_index_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_doc_count IS NULL THEN '-'
    WHEN io.diff_total_bytes > 0
      THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2))
    WHEN io.diff_total_bytes < 0
      THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt,
  ROUND(io.last_physical_bytes / POW(1024, 3), 2) AS last_physical_gb,
  ROUND(io.prev_physical_bytes / POW(1024, 3), 2) AS prev_physical_gb
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    cur.instance_id,
    cur.db_name,
    cur.coll_name,
    last_inst.stat_time AS last_stat_time,
    prev_inst.stat_time AS prev_stat_time,
    cur.doc_count AS last_doc_count,
    prev.doc_count AS prev_doc_count,
    (cur.doc_count - prev.doc_count) AS diff_doc_count,
    cur.data_bytes AS last_data_bytes,
    prev.data_bytes AS prev_data_bytes,
    (cur.data_bytes - prev.data_bytes) AS diff_data_bytes,
    cur.index_bytes AS last_index_bytes,
    prev.index_bytes AS prev_index_bytes,
    (cur.index_bytes - prev.index_bytes) AS diff_index_bytes,
    cur.logical_total_bytes AS last_total_bytes,
    prev.logical_total_bytes AS prev_total_bytes,
    (cur.logical_total_bytes - prev.logical_total_bytes) AS diff_total_bytes,
    cur.physical_total_bytes AS last_physical_bytes,
    prev.physical_total_bytes AS prev_physical_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) t
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} last_inst
    ON last_inst.instance_id = t.instance_id
   AND last_inst.stat_time = t.last_time
   AND last_inst.collect_status = 'ok'
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_INSTANCE_STORAGE} prev_inst
    ON prev_inst.instance_id = t.instance_id
   AND prev_inst.stat_time = (
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
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_COLLECTION_TOPN} cur
    ON cur.instance_id = t.instance_id
   AND cur.stat_time = last_inst.stat_time
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MONGO_COLLECTION_TOPN} prev
    ON prev.instance_id = t.instance_id
   AND prev.db_name = cur.db_name
   AND prev.coll_name = cur.coll_name
   AND prev.stat_time = prev_inst.stat_time
) AS io
ORDER BY
  CASE
    WHEN io.prev_doc_count IS NULL THEN 0
    ELSE ABS(io.last_doc_count - io.prev_doc_count)
  END DESC,
  io.env,
  io.alias_name,
  io.instance_name,
  io.db_name,
  io.coll_name
LIMIT 10;
