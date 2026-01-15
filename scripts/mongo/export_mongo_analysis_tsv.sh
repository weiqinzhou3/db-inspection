#!/usr/bin/env bash
set -euo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

projectDir=$(cd "$(dirname "$0")/../.." && pwd)

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi
# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"
OUT_DIR="${OPS_TSV_OUTDIR:-${projectDir}/out/mongo/analysis}"
mkdir -p "${OUT_DIR}"

MYSQL_OPTS=(--login-path="$OPS_META_LOGIN_PATH" --batch --raw -D "$DB_NAME")

render_sql() {
  sed \
    -e "s/\\\${OPS_INSPECTION_DB}/$OPS_INSPECTION_DB/g" \
    -e "s/\\\${T_ASSET_INSTANCE}/$T_ASSET_INSTANCE/g" \
    -e "s/\\\${T_SNAP_MONGO_INSTANCE_STORAGE}/$T_SNAP_MONGO_INSTANCE_STORAGE/g" \
    -e "s/\\\${T_SNAP_MONGO_COLLECTION_TOPN}/$T_SNAP_MONGO_COLLECTION_TOPN/g" \
    "$1"
}

# Q1
render_sql "${projectDir}/sql/mongo/analysis/q1_instance_latest.sql" | mysql "${MYSQL_OPTS[@]}" > "${OUT_DIR}/q1_instance_latest.tsv"
echo "exported Q1 to ${OUT_DIR}/q1_instance_latest.tsv"

# Q2
render_sql "${projectDir}/sql/mongo/analysis/q2_env_summary.sql" | mysql "${MYSQL_OPTS[@]}" > "${OUT_DIR}/q2_env_summary.tsv"
echo "exported Q2 to ${OUT_DIR}/q2_env_summary.tsv"

# Q3
render_sql "${projectDir}/sql/mongo/analysis/q3_instance_last_vs_prev.sql" | mysql "${MYSQL_OPTS[@]}" > "${OUT_DIR}/q3_instance_last_vs_prev.tsv"
echo "exported Q3 to ${OUT_DIR}/q3_instance_last_vs_prev.tsv"

# Q4
render_sql "${projectDir}/sql/mongo/analysis/q4_collection_latest_topn.sql" | mysql "${MYSQL_OPTS[@]}" > "${OUT_DIR}/q4_collection_latest_topn.tsv"
echo "exported Q4 to ${OUT_DIR}/q4_collection_latest_topn.tsv"

# Q5
render_sql "${projectDir}/sql/mongo/analysis/q5_collection_diff.sql" | mysql "${MYSQL_OPTS[@]}" > "${OUT_DIR}/q5_collection_diff.tsv"
echo "exported Q5 to ${OUT_DIR}/q5_collection_diff.tsv"

echo "All MongoDB analysis TSV exports completed."
