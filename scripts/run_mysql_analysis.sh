#!/usr/bin/env bash
set -euo pipefail

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-ops_meta}"
OPS_META_DB="${OPS_META_DB:-ops_inspection}"

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mysql --login-path="$OPS_META_LOGIN_PATH" -D "$OPS_META_DB" < "$root_dir/sql/analysis.sql"
