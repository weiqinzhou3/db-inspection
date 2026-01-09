#!/usr/bin/env bash
set -euo pipefail

# Initialize MongoDB assets into ops_inspection.asset_instance.
#
# Usage:
#   MONGO_AES_KEY_HEX=... MONGO_AES_IV_HEX=... \
#   OPS_META_LOGIN_PATH=ops_meta [OPS_META_DB=ops_inspection] [CONFIG_PATH=...] ./scripts/mongo/init_mongo_assets.sh

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"
CONFIG_PATH="${CONFIG_PATH:-}"
MONGO_AES_KEY_HEX="${MONGO_AES_KEY_HEX:-}"
MONGO_AES_IV_HEX="${MONGO_AES_IV_HEX:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

if [[ -z "$MONGO_AES_KEY_HEX" || -z "$MONGO_AES_IV_HEX" ]]; then
  echo "MONGO_AES_KEY_HEX and MONGO_AES_IV_HEX are required for Mongo URI encryption" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }
command -v openssl >/dev/null || { echo "openssl not found in PATH" >&2; exit 1; }

projectDir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -z "$CONFIG_PATH" ]]; then
  if [[ -f "${projectDir}/config/mongo/mongo-init.yaml" ]]; then
    CONFIG_PATH="${projectDir}/config/mongo/mongo-init.yaml"
  elif [[ -f "${projectDir}/config/mongo-init.yaml" ]]; then
    CONFIG_PATH="${projectDir}/config/mongo-init.yaml"
  else
    CONFIG_PATH="${projectDir}/config/mongo/mongo-init.yaml"
  fi
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  echo "Copy config/mongo/mongo-init.yaml.example to $CONFIG_PATH and fill values." >&2
  exit 1
fi

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

encrypt_mongo_uri() {
  local uri="$1"
  printf "%s" "$uri" | openssl enc -aes-256-cbc -base64 -K "$MONGO_AES_KEY_HEX" -iv "$MONGO_AES_IV_HEX"
}

records="$(parse_yaml "$CONFIG_PATH")"

echo "Starting Mongo asset initialization from $CONFIG_PATH"
while IFS= read -r line; do
  instance_name=""
  alias_name=""
  env=""
  type=""
  is_active="1"
  mongo_uri=""

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
      mongo_uri)     mongo_uri="$val" ;;
    esac
  done

  if [[ -z "$instance_name" ]]; then
    continue
  fi

  if [[ -n "$type" && "$type" != "mongo" ]]; then
    echo "[SKIP] instance_name=$instance_name type=$type (expect mongo)" >&2
    continue
  fi

  if [[ -z "$mongo_uri" ]]; then
    echo "[SKIP] Missing mongo_uri for instance_name=$instance_name" >&2
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

  if [[ -z "$env" ]]; then
    env_condition="env IS NULL"
  else
    env_condition="env='$(sql_escape "$env")'"
  fi

  host_val=""
  port_val=""
  if [[ "$mongo_uri" == mongodb* ]]; then
    IFS=$'\t' read -r host_val port_val <<<"$(parse_mongo_host_port "$mongo_uri")"
  fi
  if [[ -z "$host_val" ]]; then
    echo "[SKIP] Failed to parse host from mongo_uri for instance_name=$instance_name" >&2
    continue
  fi
  if [[ -z "$port_val" ]]; then
    port_val="0"
  fi

  existing_row="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "
SELECT instance_id, COALESCE(login_path,'') FROM ${T_ASSET_INSTANCE}
WHERE type='mongo'
  AND ${env_condition}
  AND host='$(sql_escape "$host_val")'
  AND port=${port_val}
  AND instance_name='$(sql_escape "$instance_name")'
LIMIT 1;
")"

  instance_id=""
  existing_login_path=""
  if [[ -n "$existing_row" ]]; then
    IFS=$'\t' read -r instance_id existing_login_path <<<"$existing_row"

    enc_uri="$(encrypt_mongo_uri "$mongo_uri")"
    if [[ -z "$enc_uri" ]]; then
      echo "[SKIP] Encrypt mongo_uri failed for instance_name=$instance_name" >&2
      continue
    fi

    mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "
UPDATE ${T_ASSET_INSTANCE}
SET is_active=${is_active},
    login_path='$(sql_escape "$enc_uri")',
    alias_name=${alias_sql},
    env=${env_sql},
    host='$(sql_escape "$host_val")',
    port=${port_val},
    auth_mode='mongo_uri_aes'
WHERE instance_id=${instance_id};
"
    echo "[UPDATE] asset_instance instance_name=$instance_name env=${env:-null} alias=${alias_name:-null} instance_id=$instance_id"
  else
    enc_uri="$(encrypt_mongo_uri "$mongo_uri")"
    if [[ -z "$enc_uri" ]]; then
      echo "[SKIP] Encrypt mongo_uri failed for instance_name=$instance_name" >&2
      continue
    fi

    insert_sql="
INSERT INTO ${T_ASSET_INSTANCE}(
  type, instance_name, alias_name, env, host, port, auth_mode, login_path, is_active
) VALUES (
  'mongo',
  '$(sql_escape "$instance_name")',
  $alias_sql,
  $env_sql,
  '$(sql_escape "$host_val")',
  ${port_val},
  'mongo_uri_aes',
  '$(sql_escape "$enc_uri")',
  ${is_active}
);
SELECT LAST_INSERT_ID();
"
    new_id="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "$insert_sql")"
    instance_id="$new_id"
    echo "[INSERT] asset_instance instance_name=$instance_name env=${env:-null} alias=${alias_name:-null} instance_id=$instance_id"
  fi

done <<< "$records"

echo "Initialization completed."
