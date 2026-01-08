#!/usr/bin/env bash
set -euo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-ops_meta}"
OPS_META_DB="${OPS_META_DB:-}"

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

projectDir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"

mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" < "$projectDir/sql/analysis.sql"