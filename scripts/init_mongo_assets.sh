#!/usr/bin/env bash
set -euo pipefail

# Initialize MongoDB assets into ops_inspection.asset_instance and asset_mongo_conn.
#
# Usage:
#   OPS_META_LOGIN_PATH=ops_meta [OPS_META_DB=ops_inspection] [CONFIG_PATH=config/mongo-init.yaml] ./scripts/init_mongo_assets.sh

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"
CONFIG_PATH="${CONFIG_PATH:-config/mongo-init.yaml}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  echo "Copy config/mongo-init.yaml.example to $CONFIG_PATH and fill values." >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }

projectDir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! -f "${projectDir}/config/schema_env.sh" ]]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"

sql_escape() {
  local s="${1//\\/\\\\}"
  s="${s//\'/\'\'}"
  printf "%s" "$s"
}

strip_quotes() {
  local s="$1"
  if [[ "$s" == \"*\" && "$s" == *\" ]]; then
    s="${s:1:${#s}-2}"
  elif [[ "$s" == \'*\' && "$s" == *\' ]]; then
    s="${s:1:${#s}-2}"
  fi
  printf "%s" "$s"
}

parse_yaml() {
  local file="$1"
  awk '
  function trim(s) {
    sub(/^[ \t\r\n]+/, "", s)
    sub(/[ \t\r\n]+$/, "", s)
    return s
  }

  function flush() {
    if (!record_started) {
      return
    }
    out = ""
    for (k in fields) {
      if (out != "") {
        out = out " "
      }
      out = out k "=" fields[k]
    }
    if (out != "") {
      print out
    }
    delete fields
    record_started = 0
  }

  BEGIN {
    record_started = 0
    delete fields
  }

  /^[ \t]*#/   { next }
  /^[ \t]*$/   { next }
  /^[ \t]*---/ { next }

  /^[ \t]*-[ \t]*/ {
    flush()
    line = $0
    sub(/^[ \t]*-[ \t]*/, "", line)
    line = trim(line)
    if (line == "") {
      record_started = 1
      next
    }
    idx = index(line, ":")
    if (idx > 0) {
      key = substr(line, 1, idx - 1)
      val = substr(line, idx + 1)
      key = trim(key)
      val = trim(val)
      fields[key] = val
      record_started = 1
    }
    next
  }

  {
    if (!record_started) {
      record_started = 1
    }
    line = $0
    sub(/^[ \t]+/, "", line)
    idx = index(line, ":")
    if (idx == 0) {
      next
    }

    key = substr(line, 1, idx - 1)
    val = substr(line, idx + 1)
    key = trim(key)
    val = trim(val)

    if (key != "") {
      fields[key] = val
    }
  }

  END {
    flush()
  }
  ' "$file"
}

parse_mongo_host_port() {
  local uri="$1"
  local host="" port=""
  local rest="$uri"

  if [[ "$rest" == mongodb://* ]]; then
    rest="${rest#mongodb://}"
  elif [[ "$rest" == mongodb+srv://* ]]; then
    rest="${rest#mongodb+srv://}"
  else
    printf "%s\t%s" "" ""
    return 0
  fi

  if [[ "$rest" == *"@"* ]]; then
    rest="${rest#*@}"
  fi

  rest="${rest%%/*}"
  rest="${rest%%\?*}"

  local first="${rest%%,*}"
  if [[ "$first" == *":"* ]]; then
    host="${first%%:*}"
    port="${first##*:}"
  else
    host="$first"
    port=""
  fi

  printf "%s\t%s" "$host" "$port"
}

records="$(parse_yaml "$CONFIG_PATH")"

echo "Starting Mongo asset initialization from $CONFIG_PATH"
while IFS= read -r line; do
  instance_name=""
  alias_name=""
  env=""
  type=""
  is_active="1"
  conn_name=""
  mongo_uri_enc=""

  for kv in $line; do
    key="${kv%%=*}"
    val="${kv#*=}"
    val="$(strip_quotes "$val")"
    case "$key" in
      instance_name) instance_name="$val" ;;
      alias_name)    alias_name="$val" ;;
      env)           env="$val" ;;
      type)          type="$val" ;;
      is_active)     is_active="$val" ;;
      conn_name)     conn_name="$val" ;;
      mongo_uri_enc) mongo_uri_enc="$val" ;;
    esac
  done

  if [[ -z "$instance_name" ]]; then
    continue
  fi

  if [[ -n "$type" && "$type" != "mongodb" ]]; then
    echo "[SKIP] instance_name=$instance_name type=$type (expect mongodb)" >&2
    continue
  fi

  if [[ -z "$mongo_uri_enc" ]]; then
    echo "[SKIP] Missing mongo_uri_enc for instance_name=$instance_name" >&2
    continue
  fi

  case "$env" in
    ""|MOS|Purple|RTM|MIB2) ;;
    *)
      echo "[SKIP] Invalid env=$env for instance_name=$instance_name" >&2
      continue
      ;;
  esac

  alias_sql="NULL"
  [[ -n "$alias_name" ]] && alias_sql="'$(sql_escape "$alias_name")'"

  env_sql="NULL"
  [[ -n "$env" ]] && env_sql="'$(sql_escape "$env")'"

  conn_name_sql="NULL"
  [[ -n "$conn_name" ]] && conn_name_sql="'$(sql_escape "$conn_name")'"

  if [[ -z "$conn_name" ]]; then
    conn_name="$instance_name"
    conn_name_sql="'$(sql_escape "$conn_name")'"
  fi

  if [[ -z "$env" ]]; then
    env_condition="env IS NULL"
  else
    env_condition="env='$(sql_escape "$env")'"
  fi

  if [[ -z "$alias_name" ]]; then
    alias_condition="(alias_name IS NULL OR alias_name='')"
  else
    alias_condition="alias_name='$(sql_escape "$alias_name")'"
  fi

  host_val=""
  port_val=""
  if [[ "$mongo_uri_enc" == mongodb* ]]; then
    IFS=$'\t' read -r host_val port_val <<<"$(parse_mongo_host_port "$mongo_uri_enc")"
  fi
  if [[ -z "$host_val" ]]; then
    host_val="-"
  fi
  if [[ -z "$port_val" ]]; then
    port_val="0"
  fi

  existing_row="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "
SELECT instance_id, COALESCE(login_path,'') FROM ${T_ASSET_INSTANCE}
WHERE type='mongodb'
  AND ${env_condition}
  AND ${alias_condition}
  AND instance_name='$(sql_escape "$instance_name")'
LIMIT 1;
")"

  instance_id=""
  existing_login_path=""
  if [[ -n "$existing_row" ]]; then
    IFS=$'\t' read -r instance_id existing_login_path <<<"$existing_row"

    mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "
UPDATE ${T_ASSET_INSTANCE}
SET is_active=${is_active},
    login_path=${conn_name_sql},
    auth_mode='mongo_uri_aes'
WHERE instance_id=${instance_id};
"
    echo "[UPDATE] asset_instance instance_name=$instance_name env=${env:-null} alias=${alias_name:-null} instance_id=$instance_id"
  else
    insert_sql="
INSERT INTO ${T_ASSET_INSTANCE}(
  type, instance_name, alias_name, env, host, port, auth_mode, login_path, is_active
) VALUES (
  'mongodb',
  '$(sql_escape "$instance_name")',
  $alias_sql,
  $env_sql,
  '$(sql_escape "$host_val")',
  ${port_val},
  'mongo_uri_aes',
  ${conn_name_sql},
  ${is_active}
);
SELECT LAST_INSERT_ID();
"
    new_id="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "$insert_sql")"
    instance_id="$new_id"
    echo "[INSERT] asset_instance instance_name=$instance_name env=${env:-null} alias=${alias_name:-null} instance_id=$instance_id"
  fi

  mongo_uri_enc_sql="'$(sql_escape "$mongo_uri_enc")'"

  mongo_conn_sql="
INSERT INTO ${T_ASSET_MONGO_CONN}(
  instance_id, conn_name, mongo_uri_enc
) VALUES (
  ${instance_id},
  ${conn_name_sql},
  ${mongo_uri_enc_sql}
)
ON DUPLICATE KEY UPDATE
  conn_name=VALUES(conn_name),
  mongo_uri_enc=VALUES(mongo_uri_enc);
"

  mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "$mongo_conn_sql"
  echo "[UPSERT] asset_mongo_conn instance_id=$instance_id conn_name=$conn_name"

done <<< "$records"

echo "Initialization completed."
