#!/usr/bin/env bash
set -euo pipefail

# Initialize MySQL assets into ops_inspection.asset_instance and create login-path entries.
# Requirements:
#   - mysql client and mysql_config_editor available
#
# Usage:
#   OPS_META_LOGIN_PATH=ops_meta [OPS_META_DB=ops_inspection] [CONFIG_PATH=config/mysql-init.yaml] ./scripts/init_mysql_assets.sh

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-ops_inspection}"
CONFIG_PATH="${CONFIG_PATH:-config/mysql-init.yaml}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  echo "Copy config/mysql-init.yaml.example to $CONFIG_PATH and fill values." >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }
command -v mysql_config_editor >/dev/null || { echo "mysql_config_editor not found in PATH" >&2; exit 1; }
command -v expect >/dev/null || { echo "expect not found in PATH (required for mysql_config_editor automation)" >&2; exit 1; }

sql_escape() {
  # Escape single quotes for SQL literals
  local s="${1//\'/\'\'}"
  printf "%s" "$s"
}

create_login_path() {
  local lp="$1" host="$2" user="$3" port="$4" password="$5"
  expect <<EOF
log_user 0
spawn mysql_config_editor set --login-path "$lp" --host "$host" --user "$user" --port "$port" --password --skip-warn
expect {
  -re ".*Enter password.*" { send "$password\r" }
  timeout { exit 1 }
}
expect eof
EOF
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "[ERROR] mysql_config_editor failed for login_path=$lp host=$host port=$port user=$user" >&2
  fi
  return $rc
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
    if (record_started) {
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", inst, alias, env, host, port, user, pass, lp
    }
    inst = ""; alias = ""; env = ""; host = ""; port = ""; user = ""; pass = ""; lp = ""
    record_started = 0
  }

  BEGIN {
    inst = ""; alias = ""; env = ""; host = ""; port = ""; user = ""; pass = ""; lp = ""
    record_started = 0
  }

  # 跳过注释行
  /^[ \t]*#/ { next }

  # 跳过空行
  /^[ \t]*$/ { next }

  {
    line = $0

    # 以 "-" 开头，表示新记录开始
    if (line ~ /^[ \t]*-/) {
      if (record_started) {
        flush()
      }
      record_started = 1
      sub(/^[ \t]*-[ \t]*/, "", line)
    }

    # 解析 key: value，只按第一个冒号分割
    idx = index(line, ":")
    if (idx > 0) {
      key = substr(line, 1, idx - 1)
      val = substr(line, idx + 1)
      key = trim(key)
      val = trim(val)

      if (key == "instance_name")      inst  = val
      else if (key == "alias_name")    alias = val
      else if (key == "env")           env   = val
      else if (key == "host")          host  = val
      else if (key == "port")          port  = val
      else if (key == "username")      user  = val
      else if (key == "password")      pass  = val
      else if (key == "login_path")    lp    = val
    }
  }

  END {
    if (record_started) {
      flush()
    }
  }
  ' "$file"
}

records="$(parse_yaml "$CONFIG_PATH")"

echo "Starting MySQL asset initialization from $CONFIG_PATH"
while IFS=$'\t' read -r instance_name alias_name env host port username password login_path; do
  if [[ -z "$instance_name" ]]; then
    continue
  fi
  if [[ -z "$env" || -z "$host" || -z "$port" || -z "$username" ]]; then
    echo "[SKIP] Missing required field for instance_name=$instance_name" >&2
    continue
  fi
  case "$env" in
    MOS|Purple|RTM|MIB2) ;;
    *) echo "[SKIP] Invalid env=$env for instance_name=$instance_name" >&2; continue ;;
  esac

  alias_sql="NULL"
  [[ -n "$alias_name" ]] && alias_sql="'$(sql_escape "$alias_name")'"
  username_sql="NULL"
  [[ -n "$username" ]] && username_sql="'$(sql_escape "$username")'"
  login_path_sql="NULL"
  [[ -n "$login_path" ]] && login_path_sql="'$(sql_escape "$login_path")'"

  existing_row="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$OPS_META_DB" -e "
SELECT instance_id, COALESCE(login_path,'') FROM asset_instance
WHERE type='mysql' AND env='$(sql_escape "$env")' AND host='$(sql_escape "$host")' AND port=$port AND instance_name='$(sql_escape "$instance_name")'
LIMIT 1;
")"

  instance_id=""
  existing_login_path=""
  if [[ -n "$existing_row" ]]; then
    IFS=$'\t' read -r instance_id existing_login_path <<<"$existing_row"
    echo "[SKIP] Exists instance_name=$instance_name env=$env host=$host port=$port (instance_id=$instance_id, login_path=${existing_login_path:-null})"
    if [[ -z "$existing_login_path" ]]; then
      login_path_final="$login_path"
      if [[ -z "$login_path_final" ]]; then
        login_path_final="i${instance_id}"
      fi
      mysql --login-path="$OPS_META_LOGIN_PATH" -D "$OPS_META_DB" -e "
UPDATE asset_instance
SET login_path='$(sql_escape "$login_path_final")'
WHERE instance_id=$instance_id;
"
      existing_login_path="$login_path_final"
      echo "[UPDATE] Set login_path=$login_path_final for instance_id=$instance_id"
    fi
  else
    insert_sql="
INSERT INTO asset_instance(
  instance_name, alias_name, env, host, port, username, login_path
) VALUES (
  '$(sql_escape "$instance_name")',
  $alias_sql,
  '$(sql_escape "$env")',
  '$(sql_escape "$host")',
  $port,
  $username_sql,
  $login_path_sql
);
SELECT LAST_INSERT_ID();
"
    instance_id="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$OPS_META_DB" -e "$insert_sql")"
    if [[ -z "$instance_id" ]]; then
      echo "[ERROR] Failed to insert asset for $instance_name" >&2
      exit 1
    fi
    echo "[NEW] Inserted instance_name=$instance_name env=$env host=$host port=$port -> instance_id=$instance_id"
    existing_login_path="$login_path"
  fi

  login_path_final="$existing_login_path"
  if [[ -z "$login_path_final" ]]; then
    if [[ -n "$login_path" ]]; then
      login_path_final="$login_path"
    else
      login_path_final="i${instance_id}"
      mysql --login-path="$OPS_META_LOGIN_PATH" -D "$OPS_META_DB" -e "
UPDATE asset_instance SET login_path='$(sql_escape "$login_path_final")' WHERE instance_id=$instance_id;
"
      echo "[UPDATE] Generated login_path=$login_path_final for instance_id=$instance_id"
    fi
  fi

  if [[ -z "$password" ]]; then
    echo "[WARN] Missing password for instance_id=$instance_id; skipping mysql_config_editor" >&2
    continue
  fi

  if ! create_login_path "$login_path_final" "$host" "$username" "$port" "$password"; then
    echo "[ERROR] Failed to create login_path=$login_path_final for instance_id=$instance_id" >&2
  else
    echo "[LOGIN_PATH] mysql_config_editor set --login-path=$login_path_final (host=$host port=$port user=$username)"
  fi

done <<< "$records"

echo "Initialization completed."
