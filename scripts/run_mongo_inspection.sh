#!/usr/bin/env bash
set -euo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"
MONGO_AES_KEY_HEX="${MONGO_AES_KEY_HEX:-}"
MONGO_AES_IV_HEX="${MONGO_AES_IV_HEX:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

if [[ -z "$MONGO_AES_KEY_HEX" || -z "$MONGO_AES_IV_HEX" ]]; then
  echo "MONGO_AES_KEY_HEX and MONGO_AES_IV_HEX are required for Mongo URI decryption" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }
command -v mongo >/dev/null || { echo "mongo shell not found in PATH" >&2; exit 1; }
command -v openssl >/dev/null || { echo "openssl not found in PATH" >&2; exit 1; }

projectDir="$(
  cd "$(dirname "$0")/.."
  pwd
)"

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
echo "[INIT] T_ASSET_MONGO_CONN=${T_ASSET_MONGO_CONN}"
echo "[INIT] T_SNAP_MONGO_INSTANCE_STORAGE=${T_SNAP_MONGO_INSTANCE_STORAGE}"
echo "[INIT] T_SNAP_MONGO_COLLECTION_TOPN=${T_SNAP_MONGO_COLLECTION_TOPN}"

if ! mysql --login-path="${OPS_META_LOGIN_PATH}" -e "SOURCE ${projectDir}/sql/ddl.sql" >/dev/null 2>&1; then
  echo "[ERROR] Failed to ensure schema/tables via sql/ddl.sql" >&2
  exit 1
fi

sql_escape() {
  local s="$1"
  s=${s//\\/\\\\}
  s=${s//\'/\'\'}
  printf '%s' "$s"
}

decrypt_mongo_uri() {
  local enc="$1"
  printf "%s" "$enc" | openssl enc -d -aes-256-cbc -base64 -K "$MONGO_AES_KEY_HEX" -iv "$MONGO_AES_IV_HEX"
}

success=0
failed=0
has_assets=false

echo "[FETCH] Loading active MongoDB assets from ${DB_NAME}.${T_ASSET_INSTANCE}"

while IFS=$'\t' read -r instance_id env alias_name instance_name conn_name mongo_uri_enc; do
  if [[ -z "${instance_id:-}" ]]; then
    continue
  fi
  has_assets=true

  echo "================ INSTANCE BEGIN ================"
  echo "[RUN] instance_id=${instance_id} env=${env:-} alias=${alias_name:-} instance=${instance_name:-} conn_name=${conn_name:-}"

  instance_error=""
  stat_time=""
  logical_data=0
  logical_index=0
  logical_total=0
  physical_total=0
  mongo_version=""

  uri=""
  dec_out=""
  dec_rc=0
  set +e
  dec_out="$(decrypt_mongo_uri "$mongo_uri_enc" 2>&1)"
  dec_rc=$?
  set -e
  if [[ $dec_rc -ne 0 || -z "$dec_out" ]]; then
    instance_error="decrypt_failed: ${dec_out}"
  else
    uri="$dec_out"
  fi

  summary_out=""
  summary_rc=0
  if [[ -n "$uri" ]]; then
    set +e
    summary_out="$(mongo "$uri" --quiet "${projectDir}/mongo_scripts/collect_instance_capacity.js" 2>&1)"
    summary_rc=$?
    set -e
  else
    summary_rc=1
  fi

  if [[ ${summary_rc} -ne 0 || -z "${summary_out}" ]]; then
    if [[ -n "${instance_error}" ]]; then
      instance_error+=$'; '
    fi
    instance_error+="summary_failed: ${summary_out}"
  else
    summary_line="$(printf "%s\n" "$summary_out" | head -n 1)"
    IFS=$'\t' read -r stat_time logical_data logical_index logical_total physical_total mongo_version <<<"${summary_line}"
  fi

  topn_out=""
  topn_rc=0
  if [[ -n "$uri" ]]; then
    set +e
    topn_out="$(mongo "$uri" --quiet "${projectDir}/mongo_scripts/collect_collection_capacity.js" 2>&1)"
    topn_rc=$?
    set -e
  else
    topn_rc=1
  fi

  if [[ ${topn_rc} -ne 0 ]]; then
    if [[ -n "${instance_error}" ]]; then
      instance_error+=$'; '
    fi
    instance_error+="collection_failed: ${topn_out}"
  fi

  collect_status="ok"
  error_msg_sql="NULL"
  mongo_version_sql="NULL"

  if [[ ${summary_rc} -ne 0 || -z "${stat_time}" ]]; then
    collect_status="failed"
    stat_time="$(date '+%Y-%m-%d %H:%M:%S')"
  fi

  if [[ -n "${instance_error}" ]]; then
    collect_status="failed"
    error_msg_sql="'$(sql_escape "${instance_error}")'"
  fi

  if [[ -n "${mongo_version}" ]]; then
    mongo_version_sql="'$(sql_escape "${mongo_version}")'"
  fi

  instance_insert_sql=$(
    cat <<EOF
INSERT INTO ${T_SNAP_MONGO_INSTANCE_STORAGE}(
  stat_time,
  instance_id,
  logical_data_bytes,
  logical_index_bytes,
  logical_total_bytes,
  physical_total_bytes,
  mongo_version,
  collect_status,
  error_msg
) VALUES (
  '${stat_time}',
  '${instance_id}',
  ${logical_data:-0},
  ${logical_index:-0},
  ${logical_total:-0},
  ${physical_total:-0},
  ${mongo_version_sql},
  '${collect_status}',
  ${error_msg_sql}
);
EOF
  )

  if mysql --login-path="${OPS_META_LOGIN_PATH}" -D "${DB_NAME}" -e "${instance_insert_sql}" >/dev/null 2>&1; then
    :
  else
    echo "[ERROR] Insert into ${T_SNAP_MONGO_INSTANCE_STORAGE} failed for instance_id=${instance_id}" >&2
  fi

  if [[ "${collect_status}" == "ok" && ${topn_rc} -eq 0 && -n "${topn_out}" ]]; then
    while IFS=$'\t' read -r t_stat db_name coll_name doc_count data_bytes index_bytes logical_total_bytes physical_total_bytes; do
      if [[ -z "${db_name:-}" || -z "${coll_name:-}" ]]; then
        continue
      fi

      esc_db_name="$(sql_escape "${db_name}")"
      esc_coll_name="$(sql_escape "${coll_name}")"

      topn_insert_sql=$(
        cat <<EOF
INSERT INTO ${T_SNAP_MONGO_COLLECTION_TOPN}(
  stat_time,
  instance_id,
  db_name,
  coll_name,
  doc_count,
  data_bytes,
  index_bytes,
  logical_total_bytes,
  physical_total_bytes
) VALUES (
  '${t_stat}',
  '${instance_id}',
  '${esc_db_name}',
  '${esc_coll_name}',
  ${doc_count:-0},
  ${data_bytes:-0},
  ${index_bytes:-0},
  ${logical_total_bytes:-0},
  ${physical_total_bytes:-0}
);
EOF
      )

      mysql --login-path="${OPS_META_LOGIN_PATH}" -D "${DB_NAME}" -e "${topn_insert_sql}" >/dev/null 2>&1 || true
    done <<< "${topn_out}"
  fi

  if [[ "${collect_status}" == "ok" ]]; then
    ((success++))
  else
    ((failed++))
  fi

  echo "[RESULT] instance_id=${instance_id} conn_name=${conn_name:-} => ${collect_status}"
  echo "================ INSTANCE END =================="
  echo
done < <(
  mysql --login-path="${OPS_META_LOGIN_PATH}" \
        --batch --raw -N \
        -D "${DB_NAME}" \
        -e "
SELECT i.instance_id,
       COALESCE(i.env, '') AS env,
       COALESCE(i.alias_name, '') AS alias_name,
       COALESCE(i.instance_name, '') AS instance_name,
       COALESCE(c.conn_name, '') AS conn_name,
       c.mongo_uri_enc
FROM ${T_ASSET_INSTANCE} i
JOIN ${T_ASSET_MONGO_CONN} c ON c.instance_id = i.instance_id
WHERE i.type='mongodb'
  AND i.is_active=1;
"
)

if [[ "${has_assets}" != "true" ]]; then
  echo "[WARN] No active MongoDB assets found."
fi

echo "[SUMMARY] mongodb instances: success=${success}, failed=${failed}"
