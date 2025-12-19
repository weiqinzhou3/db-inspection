#!/usr/bin/env bash
set -euo pipefail

# Batch MySQL inspection using existing collect/load SQL.
# Requirements: mysql client available; OPS_META_LOGIN_PATH env must be set.
#
# Usage:
#   OPS_META_LOGIN_PATH=ops_meta [OPS_META_DB=ops_inspection] ./scripts/run_mysql_inspection.sh

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

projectDir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"

sql_escape() {
  local s="${1//\'/\'\'}"
  printf "%s" "$s"
}

success=0
failed=0

echo "[INIT] Ensure schema/tables exist via sql/ddl.sql"
mysql --login-path="$OPS_META_LOGIN_PATH" < "$projectDir/sql/ddl.sql"

echo "[FETCH] Loading active MySQL assets from $DB_NAME.${T_ASSET_INSTANCE}"
assets="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "
SELECT instance_id, login_path
FROM ${T_ASSET_INSTANCE}
WHERE type='mysql' AND is_active=1 AND auth_mode='login_path' AND login_path IS NOT NULL;
")"

if [[ -z "$assets" ]]; then
  echo "[INFO] No active MySQL assets to inspect."
  exit 0
fi

while IFS=$'\t' read -r instance_id login_path; do
  [[ -z "$instance_id" ]] && continue
  echo "[RUN] instance_id=$instance_id login_path=$login_path"
  instance_error=""
  stat_time=""
  logical_data="0"
  logical_index="0"
  logical_total="0"
  mysql_version=""

  # Collect instance summary
  summary_out=""
  if ! summary_out="$(
    {
      printf 'SET @instance_id:=%s;\n' "$instance_id"
      cat "$projectDir/sql/collect/instance_logical_summary.sql"
    } | mysql --login-path="$login_path" --batch --raw -N 2>&1
  )"; then
    instance_error="summary: $summary_out"
    stat_time="$(date '+%F %T')"
  else
    if [[ -n "$summary_out" ]]; then
      IFS=$'\t' read -r stat_time instance_id summary_data summary_index summary_total mysql_version <<<"$summary_out"
      logical_data="${summary_data:-0}"
      logical_index="${summary_index:-0}"
      logical_total="${summary_total:-0}"
    else
      instance_error="summary returned empty result"
      stat_time="$(date '+%F %T')"
    fi
  fi

  # Collect TopN only if summary succeeded
  topn_ok=true
  if [[ -z "$instance_error" ]]; then
    if ! topn_out="$(
      {
      printf 'SET @instance_id:=%s;\n' "$instance_id"
      cat "$projectDir/sql/collect/top20_tables.sql"
    } | mysql --login-path="$login_path" --batch --raw -N 2>&1
  )"; then
      instance_error="topn: $topn_out"
      topn_ok=false
    elif [[ -n "$topn_out" ]]; then
      while IFS=$'\t' read -r t_stat_time t_instance_id schema_name table_name engine table_rows data_bytes index_bytes total_bytes rank_no; do
        [[ -z "$t_stat_time" ]] && continue
        engine_sql="NULL"
        [[ -n "$engine" ]] && engine_sql="'$(sql_escape "$engine")'"
        table_rows_sql="NULL"
        [[ -n "$table_rows" ]] && table_rows_sql="$table_rows"
        if ! mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "
INSERT INTO ${T_SNAP_MYSQL_TABLE_TOPN}(
  stat_time,instance_id,schema_name,table_name,engine,table_rows,data_bytes,index_bytes,total_bytes,rank_no
) VALUES (
  '$t_stat_time','$t_instance_id','$(sql_escape "$schema_name")','$(sql_escape "$table_name")',$engine_sql,$table_rows_sql,$data_bytes,$index_bytes,$total_bytes,$rank_no
);
"
        then
          instance_error="topn load failed for ${schema_name}.${table_name}"
          topn_ok=false
          break
        fi
      done <<< "$topn_out"
    fi
  else
    topn_ok=false
  fi

  collect_status="ok"
  error_msg="NULL"
  if [[ -n "$instance_error" ]]; then
    collect_status="failed"
    # Trim and escape error message
    error_clean="$(echo "$instance_error" | tr '\n' ' ')"
    error_clean="${error_clean:0:250}"
    error_msg="'$(sql_escape "$error_clean")'"
  fi

  mysql_version_sql="NULL"
  [[ -n "$mysql_version" ]] && mysql_version_sql="'$(sql_escape "$mysql_version")'"

  if ! mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "
INSERT INTO ${T_SNAP_MYSQL_INSTANCE_STORAGE}(
  stat_time,instance_id,logical_data_bytes,logical_index_bytes,logical_total_bytes,mysql_version,collect_status,error_msg
) VALUES (
  '$stat_time','$instance_id',$logical_data,$logical_index,$logical_total,$mysql_version_sql,'$collect_status',$error_msg
);
"
  then
    echo "[ERROR] meta insert failed for instance_id=$instance_id" >&2
    ((failed++))
    continue
  fi

  if [[ "$collect_status" == "ok" ]]; then
    ((success++))
    echo "[OK] instance_id=$instance_id (topn=${topn_ok})"
  else
    ((failed++))
    echo "[FAIL] instance_id=$instance_id msg=${instance_error}"
  fi
done <<< "$assets"

echo "[SUMMARY] success=$success failed=$failed"
