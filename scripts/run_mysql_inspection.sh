#!/usr/bin/env bash
#
# 批量 MySQL 巡检采集脚本
# - 从 meta 库的 asset_instance 里读取所有 active、login_path 模式的 MySQL 实例
# - 对每个实例：
#   * 使用对应 login_path 连接业务库，执行 collect SQL
#   * 将采集结果写入 meta 库的快照表
# - 不会因为单个实例失败就中断整个批次

set -uo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-ops_meta}"
OPS_META_DB="${OPS_META_DB:-}"

# 项目根目录
projectDir="$(
  cd "$(dirname "$0")/.."
  pwd
)"

# schema / table 映射
if [[ ! -f "${projectDir}/config/schema_env.sh" ]]; then
  echo "[ERROR] Missing ${projectDir}/config/schema_env.sh" >&2
  exit 1
fi
# shellcheck source=/dev/null
. "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"

echo "[INIT] projectDir=${projectDir}"
echo "[INIT] OPS_META_LOGIN_PATH=${OPS_META_LOGIN_PATH} DB_NAME=${DB_NAME}"
echo "[INIT] T_ASSET_INSTANCE=${T_ASSET_INSTANCE}"
echo "[INIT] T_SNAP_MYSQL_INSTANCE_STORAGE=${T_SNAP_MYSQL_INSTANCE_STORAGE}"
echo "[INIT] T_SNAP_MYSQL_TABLE_TOPN=${T_SNAP_MYSQL_TABLE_TOPN}"

# 确保 schema / tables 存在
if ! mysql --login-path="${OPS_META_LOGIN_PATH}" -e "SOURCE ${projectDir}/sql/ddl.sql" >/dev/null 2>&1; then
  echo "[ERROR] Failed to ensure schema/tables via sql/ddl.sql" >&2
  exit 1
fi

success=0
failed=0

# 简单的 SQL 字符串转义（单引号、反斜杠）
sql_escape() {
  local s="$1"
  s=${s//\\/\\\\}   # \
  s=${s//\'/\'\'}   # '
  printf '%s' "$s"
}

echo "[FETCH] Loading active MySQL assets from ${DB_NAME}.${T_ASSET_INSTANCE}"

has_assets=false

# 直接从 mysql 流式读取资产列表
while IFS=$'\t' read -r instance_id login_path; do
  # 没有数据时 read 会返回非 0；这里用 [[ -z ]] 再过滤一下空行
  if [[ -z "${instance_id:-}" ]]; then
    continue
  fi
  has_assets=true

  echo "================ INSTANCE BEGIN ================"
  echo "[RUN] instance_id=${instance_id} login_path=${login_path}"

  instance_error=""
  stat_time=""
  logical_data=0
  logical_index=0
  logical_total=0
  mysql_version=""

  # ---------- 1. 采集实例容量汇总 ----------
  summary_out="$(
    {
      printf 'SET @instance_id:=%s;\n' "${instance_id}"
      cat "${projectDir}/sql/collect/instance_logical_summary.sql"
    } | mysql --login-path="${login_path}" --batch --raw -N 2>&1
  )"
  summary_rc=$?

  if [[ ${summary_rc} -ne 0 || -z "${summary_out}" ]]; then
    # 采集失败，记录错误信息
    instance_error="summary_failed: ${summary_out}"
  else
    # 预期只有一行：stat_time, instance_id, logical_data_bytes, logical_index_bytes, logical_total_bytes, mysql_version
    IFS=$'\t' read -r stat_time _ logical_data logical_index logical_total mysql_version <<<"${summary_out}"
  fi

  # ---------- 2. 采集大表 TopN ----------
  topn_out="$(
    {
      printf 'SET @instance_id:=%s;\n' "${instance_id}"
      cat "${projectDir}/sql/collect/top_tables.sql"
    } | mysql --login-path="${login_path}" --batch --raw -N 2>&1
  )"
  topn_rc=$?

  if [[ ${topn_rc} -ne 0 ]]; then
    if [[ -n "${instance_error}" ]]; then
      instance_error+=$'; '
    fi
    instance_error+="top_tables_failed: ${topn_out}"
  fi

  collect_status="ok"
  error_msg_sql="NULL"
  mysql_version_sql="NULL"

  # 如果 summary 失败，整实例标记为 failed
  if [[ ${summary_rc} -ne 0 || -z "${stat_time}" ]]; then
    collect_status="failed"
    stat_time="$(date '+%Y-%m-%d %H:%M:%S')"
  fi

  if [[ -n "${instance_error}" ]]; then
    collect_status="failed"
    error_msg_sql="'$(sql_escape "${instance_error}")'"
  fi

  if [[ -n "${mysql_version}" ]]; then
    mysql_version_sql="'$(sql_escape "${mysql_version}")'"
  fi

  # ---------- 3. 写入实例级快照 ----------
  # 注意：即便 collect_status=failed，也写一条快照，便于 Q1 分析失败原因
  instance_insert_sql=$(
    cat <<EOF
INSERT INTO ${T_SNAP_MYSQL_INSTANCE_STORAGE}(
  stat_time,
  instance_id,
  logical_data_bytes,
  logical_index_bytes,
  logical_total_bytes,
  mysql_version,
  collect_status,
  error_msg
) VALUES (
  '${stat_time}',
  '${instance_id}',
  ${logical_data},
  ${logical_index},
  ${logical_total},
  ${mysql_version_sql},
  '${collect_status}',
  ${error_msg_sql}
);
EOF
  )

  if mysql --login-path="${OPS_META_LOGIN_PATH}" -D "${DB_NAME}" -e "${instance_insert_sql}" >/dev/null 2>&1; then
    :
  else
    echo "[ERROR] Insert into ${T_SNAP_MYSQL_INSTANCE_STORAGE} failed for instance_id=${instance_id}" >&2
    # 不再退出脚本，继续处理其他实例
  fi

  # ---------- 4. 写入大表 TopN 明细 ----------
  # 只有在整体采集 OK 且 topn_rc=0 且有结果时才写入
  if [[ "${collect_status}" == "ok" && ${topn_rc} -eq 0 && -n "${topn_out}" ]]; then
    # 每行：stat_time instance_id schema_name table_name engine table_rows data_bytes index_bytes total_bytes rank_no
    while IFS=$'\t' read -r t_stat t_inst schema_name table_name engine table_rows data_bytes index_bytes total_bytes rank_no; do
      # 防御性判断
      if [[ -z "${schema_name:-}" || -z "${table_name:-}" ]]; then
        continue
      fi

      esc_schema_name="$(sql_escape "${schema_name}")"
      esc_table_name="$(sql_escape "${table_name}")"

      engine_val="NULL"
      if [[ -n "${engine}" ]]; then
        engine_val="'$(sql_escape "${engine}")'"
      fi

      table_rows_val="NULL"
      if [[ -n "${table_rows}" ]]; then
        table_rows_val="${table_rows}"
      fi

      topn_insert_sql=$(
        cat <<EOF
INSERT INTO ${T_SNAP_MYSQL_TABLE_TOPN}(
  stat_time,
  instance_id,
  schema_name,
  table_name,
  engine,
  table_rows,
  data_bytes,
  index_bytes,
  total_bytes,
  rank_no
) VALUES (
  '${t_stat}',
  '${t_inst}',
  '${esc_schema_name}',
  '${esc_table_name}',
  ${engine_val},
  ${table_rows_val},
  ${data_bytes},
  ${index_bytes},
  ${total_bytes},
  ${rank_no}
);
EOF
      )

      # 单条失败不影响其他行和其他实例
      mysql --login-path="${OPS_META_LOGIN_PATH}" -D "${DB_NAME}" -e "${topn_insert_sql}" >/dev/null 2>&1 || true

    done <<< "${topn_out}"
  fi

  # ---------- 5. 成功 / 失败计数 ----------
  if [[ "${collect_status}" == "ok" ]]; then
    ((success++))
  else
    ((failed++))
  fi

  echo "[RESULT] instance_id=${instance_id} login_path=${login_path} => ${collect_status}"
  echo "================ INSTANCE END =================="
  echo

done < <(
  mysql --login-path="${OPS_META_LOGIN_PATH}" \
        --batch --raw -N \
        -D "${DB_NAME}" \
        -e "
SELECT instance_id, login_path
FROM ${T_ASSET_INSTANCE}
WHERE type='mysql'
  AND is_active=1
  AND auth_mode='login_path'
  AND login_path IS NOT NULL;
"
)

if ! ${has_assets}; then
  echo "[INFO] No active MySQL assets with login_path to inspect."
  exit 0
fi

echo "[SUMMARY] success=${success} failed=${failed}"
exit 0