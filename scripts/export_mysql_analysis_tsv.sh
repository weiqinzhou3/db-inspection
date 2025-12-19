#!/usr/bin/env bash
set -euo pipefail

# Export Q1~Q6 analysis results to TSV files.
# Usage:
#   OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/export_mysql_analysis_tsv.sh

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

# Q1: 失败实例列表（最新一次采集失败）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
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
JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
  ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND s.collect_status = 'failed'
ORDER BY s.stat_time DESC, a.env, a.instance_name;" \
  > "${OUT_DIR}/q1_failed_instances.tsv"

echo "exported Q1 to ${OUT_DIR}/q1_failed_instances.tsv"

# Q2: 按 env 聚合容量（最新 vs 上一次，仅成功实例）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
  env,
  COUNT(*) AS instance_count,
  ROUND(SUM(last_total_bytes) / POW(1024, 3), 2) AS last_env_total_gb,
  ROUND(SUM(prev_total_bytes) / POW(1024, 3), 2) AS prev_env_total_gb,
  CASE
    WHEN SUM(last_total_bytes - prev_total_bytes) > 0 THEN CONCAT('+', ROUND(SUM(last_total_bytes - prev_total_bytes) / POW(1024, 3), 2))
    WHEN SUM(last_total_bytes - prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(SUM(last_total_bytes - prev_total_bytes)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_env_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    last_rec.instance_id,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) lt
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
    AND last_rec.collect_status = 'ok'
) inst_ok
GROUP BY env
ORDER BY SUM(last_total_bytes - prev_total_bytes) DESC, last_env_total_gb DESC;" \
  > "${OUT_DIR}/q2_env_summary.tsv"

echo "exported Q2 to ${OUT_DIR}/q2_env_summary.tsv"

# Q3: 实例维度最近 vs 上一次容量（含 data/index/total 差异，全部成功实例）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.host,
  io.port,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_data_bytes > 0 THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2))
    WHEN io.diff_data_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_index_bytes > 0 THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2))
    WHEN io.diff_index_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_total_bytes > 0 THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2))
    WHEN io.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    a.host,
    a.port,
    last_rec.stat_time AS last_stat_time,
    prev_rec.stat_time AS prev_stat_time,
    last_rec.logical_data_bytes AS last_data_bytes,
    IFNULL(prev_rec.logical_data_bytes, 0) AS prev_data_bytes,
    (last_rec.logical_data_bytes - IFNULL(prev_rec.logical_data_bytes, 0)) AS diff_data_bytes,
    last_rec.logical_index_bytes AS last_index_bytes,
    IFNULL(prev_rec.logical_index_bytes, 0) AS prev_index_bytes,
    (last_rec.logical_index_bytes - IFNULL(prev_rec.logical_index_bytes, 0)) AS diff_index_bytes,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes,
    (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) AS diff_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
    GROUP BY instance_id
  ) lt
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
    AND last_rec.collect_status = 'ok'
) io
ORDER BY ABS(io.diff_total_bytes) DESC, io.env, io.instance_name;" \
  > "${OUT_DIR}/q3_instance_last_vs_prev.tsv"

echo "exported Q3 to ${OUT_DIR}/q3_instance_last_vs_prev.tsv"

# Q4: 库维度当前容量 Top5（每实例，最新快照，成功实例）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
  ranked.env,
  ranked.alias_name,
  ranked.instance_name,
  ranked.schema_name,
  ROUND(ranked.total_bytes / POW(1024, 3), 2) AS total_gb,
  ranked.rank_no
FROM (
  SELECT
    io.env,
    io.alias_name,
    io.instance_name,
    agg.instance_id,
    agg.schema_name,
    agg.total_bytes,
    @rk := IF(@cur_inst = agg.instance_id, @rk + 1, 1) AS rank_no,
    @cur_inst := agg.instance_id AS cur_inst
  FROM (
    SELECT
      t.instance_id,
      t.schema_name,
      SUM(t.total_bytes) AS total_bytes
    FROM (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN}
      GROUP BY instance_id
    ) lt
    JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} t
      ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
    JOIN (
      SELECT
        CAST(a.instance_id AS CHAR) AS instance_id,
        CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
        a.alias_name,
        a.instance_name
      FROM ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
      JOIN (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
        GROUP BY instance_id
      ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
      JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} ilast
        ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
      WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
    ) io ON io.instance_id = t.instance_id
    GROUP BY t.instance_id, t.schema_name
  ) agg
  JOIN (SELECT @rk := 0, @cur_inst := NULL) vars
  JOIN (
    SELECT
      CAST(a.instance_id AS CHAR) AS instance_id,
      CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
      a.alias_name,
      a.instance_name
    FROM ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
      GROUP BY instance_id
    ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
    JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} ilast
      ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
    WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
  ) io ON io.instance_id = agg.instance_id
  ORDER BY agg.instance_id, agg.total_bytes DESC, agg.schema_name
) ranked
WHERE ranked.rank_no <= 5
ORDER BY ranked.instance_name, ranked.rank_no;" \
  > "${OUT_DIR}/q4_schema_top5.tsv"

echo "exported Q4 to ${OUT_DIR}/q4_schema_top5.tsv"

# Q5: 表维度当前容量 Top10（每实例，最新快照，成功实例）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
  ranked.env,
  ranked.alias_name,
  ranked.instance_name,
  ranked.schema_name,
  ranked.table_name,
  ROUND(ranked.total_bytes / POW(1024, 3), 2) AS total_gb,
  ranked.rank_no
FROM (
  SELECT
    io.env,
    io.alias_name,
    io.instance_name,
    t.instance_id,
    t.schema_name,
    t.table_name,
    t.total_bytes,
    @rk := IF(@cur_inst = t.instance_id, @rk + 1, 1) AS rank_no,
    @cur_inst := t.instance_id AS cur_inst
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN}
    GROUP BY instance_id
  ) lt
  JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} t
    ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
  JOIN (
    SELECT
      CAST(a.instance_id AS CHAR) AS instance_id,
      CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
      a.alias_name,
      a.instance_name
    FROM ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
      GROUP BY instance_id
    ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
    JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} ilast
      ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
    WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
  ) io ON io.instance_id = t.instance_id
  JOIN (SELECT @rk := 0, @cur_inst := NULL) vars
  ORDER BY t.instance_id, t.total_bytes DESC, t.schema_name, t.table_name
) ranked
WHERE ranked.rank_no <= 10
ORDER BY ranked.instance_name, ranked.rank_no;" \
  > "${OUT_DIR}/q5_table_top10.tsv"

echo "exported Q5 to ${OUT_DIR}/q5_table_top10.tsv"

# Q6: 表维度近两次容量差异 Top10（每实例，按 diff 总量排序，成功实例）
mysql --login-path="$OPS_META_LOGIN_PATH" \
  -D "$DB_NAME" \
  --batch --raw \
  -e "SELECT
  ranked.env,
  ranked.alias_name,
  ranked.instance_name,
  ranked.schema_name,
  ranked.table_name,
  ROUND(ranked.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(ranked.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN ranked.diff_total_bytes > 0 THEN CONCAT('+', ROUND(ranked.diff_total_bytes / POW(1024, 3), 2))
    WHEN ranked.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(ranked.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    io.env,
    io.alias_name,
    io.instance_name,
    diff.instance_id,
    diff.schema_name,
    diff.table_name,
    diff.last_total_bytes,
    diff.prev_total_bytes,
    diff.diff_total_bytes,
    @rk := IF(@cur_inst = diff.instance_id, @rk + 1, 1) AS rank_no,
    @cur_inst := diff.instance_id AS cur_inst
  FROM (
    SELECT
      t_last.instance_id,
      t_last.schema_name,
      t_last.table_name,
      t_last.total_bytes AS last_total_bytes,
      IFNULL(t_prev.total_bytes, 0) AS prev_total_bytes,
      (t_last.total_bytes - IFNULL(t_prev.total_bytes, 0)) AS diff_total_bytes
    FROM (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN}
      GROUP BY instance_id
    ) lt
    JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} t_last
      ON t_last.instance_id = lt.instance_id AND t_last.stat_time = lt.last_time
    LEFT JOIN (
      SELECT s.instance_id, MAX(s.stat_time) AS prev_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} s
      JOIN (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN}
        GROUP BY instance_id
      ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
      GROUP BY s.instance_id
    ) pt ON pt.instance_id = lt.instance_id
    LEFT JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_TABLE_TOPN} t_prev
      ON t_prev.instance_id = lt.instance_id
      AND t_prev.stat_time = pt.prev_time
      AND t_prev.schema_name = t_last.schema_name
      AND t_prev.table_name = t_last.table_name
  ) diff
  JOIN (
    SELECT
      CAST(a.instance_id AS CHAR) AS instance_id,
      CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
      a.alias_name,
      a.instance_name
    FROM ${OPS_INSPECTION_DB}.${T_ASSET_INSTANCE} a
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE}
      GROUP BY instance_id
    ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
    JOIN ${OPS_INSPECTION_DB}.${T_SNAP_MYSQL_INSTANCE_STORAGE} ilast
      ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
    WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
  ) io ON io.instance_id = diff.instance_id
  JOIN (SELECT @rk := 0, @cur_inst := NULL) vars
  ORDER BY diff.instance_id, ABS(diff.diff_total_bytes) DESC, diff.diff_total_bytes DESC, diff.schema_name, diff.table_name
) ranked
WHERE ranked.rank_no <= 10
ORDER BY ranked.instance_name, ranked.rank_no;" \
  > "${OUT_DIR}/q6_table_diff_top10.tsv"

echo "exported Q6 to ${OUT_DIR}/q6_table_diff_top10.tsv"
