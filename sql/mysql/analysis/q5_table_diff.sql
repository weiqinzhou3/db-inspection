SELECT
  io.env,
  io.alias_name as instance_name,
  io.schema_name,
  io.table_name,
  CASE
    WHEN io.prev_total_bytes IS NULL THEN '-'
    WHEN (io.last_total_bytes - io.prev_total_bytes) > 0
      THEN CONCAT('+', ROUND((io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    WHEN (io.last_total_bytes - io.prev_total_bytes) < 0
      THEN CONCAT('-', ROUND(ABS(io.last_total_bytes - io.prev_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_fmt,
  CASE
    WHEN io.prev_table_rows IS NULL THEN '-'
    WHEN (io.last_table_rows - io.prev_table_rows) > 0
      THEN CONCAT('+', (io.last_table_rows - io.prev_table_rows))
    WHEN (io.last_table_rows - io.prev_table_rows) < 0
      THEN CONCAT('-', ABS(io.last_table_rows - io.prev_table_rows))
    ELSE '0'
  END AS diff_rows_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total,
  io.last_table_rows as last_rows,
  io.prev_table_rows as prev_rows,
  io.last_stat_time,
  io.prev_stat_time
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
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s_last
    ON s_last.instance_id = cur.instance_id
   AND s_last.stat_time = (
         SELECT MAX(s2.stat_time)
         FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s2
         WHERE s2.instance_id = cur.instance_id
       )
   AND s_last.collect_status = 'ok'
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
  WHERE cur.stat_time = (
          SELECT MAX(c2.stat_time)
          FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} c2
          WHERE c2.instance_id = cur.instance_id
            AND c2.schema_name = cur.schema_name
            AND c2.table_name  = cur.table_name
        )
) AS io
ORDER BY
  CASE
    WHEN io.prev_table_rows IS NULL THEN 0
    ELSE ABS(io.last_table_rows - io.prev_table_rows)
  END DESC,
  io.env,
  io.alias_name,
  io.instance_name,
  io.schema_name,
  io.table_name;
