#!/usr/bin/env bash
set -euo pipefail

projectDir=$(cd "$(dirname "$0")/.." && pwd)
DDL="${projectDir}/sql/ddl.sql"
OUT="${projectDir}/config/schema_env.sh"

if [[ ! -f "$DDL" ]]; then
  echo "FATAL: $DDL not found." >&2
  exit 1
fi

schema_lines=$(grep -E '^-- @schema ' "$DDL" || true)
table_lines=$(grep -E '^-- @table ' "$DDL" || true)

if [[ -z "$schema_lines" && -z "$table_lines" ]]; then
  echo "FATAL: no @schema/@table mappings found in $DDL" >&2
  exit 1
fi

{
  echo "# Auto-generated schema/table mapping. Do not edit."
  echo "# Source: sql/ddl.sql @schema/@table annotations."
  echo

  if [[ -n "$schema_lines" ]]; then
    while IFS= read -r line; do
      kv=${line#-- @schema }
      echo "$kv"
    done <<< "$schema_lines"
    echo
  fi

  if [[ -n "$table_lines" ]]; then
    while IFS= read -r line; do
      kv=${line#-- @table }
      key=${kv%%=*}
      value=${kv#*=}
      echo "T_${key}=${value}"
    done <<< "$table_lines"
  fi
} > "$OUT"