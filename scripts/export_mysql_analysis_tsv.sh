#!/usr/bin/env bash
set -euo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

projectDir=$(cd "$(dirname "$0")/.." && pwd)

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi
# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"
OUT_DIR="${OPS_TSV_OUTDIR:-${projectDir}/out/mysql_analysis}"
mkdir -p "${OUT_DIR}"

# Q1
mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" --batch --raw -e "$(cat <<SQL
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
SQL
)" > "${OUT_DIR}/q1_failed_instances.tsv"
echo "exported Q1 to ${OUT_DIR}/q1_failed_instances.tsv"

# Q2
mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" --batch --raw -e "$(cat <<SQL
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
SQL
)" > "${OUT_DIR}/q2_env_summary.tsv"
echo "exported Q2 to ${OUT_DIR}/q2_env_summary.tsv"

# Q3
mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" --batch --raw -e "$(cat <<SQL
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
SQL
)" > "${OUT_DIR}/q3_instance_last_vs_prev.tsv"
echo "exported Q3 to ${OUT_DIR}/q3_instance_last_vs_prev.tsv"

# Q4
mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" --batch --raw -e "$(cat <<SQL
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
SQL
)" > "${OUT_DIR}/q4_table_last.tsv"
echo "exported Q4 to ${OUT_DIR}/q4_table_last.tsv"

# Q5
mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" --batch --raw -e "$(cat <<SQL
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
SQL
)" > "${OUT_DIR}/q5_table_diff.tsv"
echo "exported Q5 to ${OUT_DIR}/q5_table_diff.tsv"

echo "All MySQL analysis TSV exports completed."