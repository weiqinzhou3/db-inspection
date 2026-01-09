#!/usr/bin/env bash
set -euo pipefail

# Initialize MySQL assets into ops_inspection.asset_instance and create login-path entries.
# Requirements:
#   - mysql client and mysql_config_editor available
#
# Usage:
#   OPS_META_LOGIN_PATH=ops_meta [OPS_META_DB=ops_inspection] [CONFIG_PATH=...] ./scripts/mysql/init_mysql_assets.sh

OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
OPS_META_DB="${OPS_META_DB:-}"
CONFIG_PATH="${CONFIG_PATH:-}"

if [[ -z "$OPS_META_LOGIN_PATH" ]]; then
  echo "OPS_META_LOGIN_PATH is required (mysql_config_editor login-path for meta DB)" >&2
  exit 1
fi

command -v mysql >/dev/null || { echo "mysql client not found in PATH" >&2; exit 1; }
command -v mysql_config_editor >/dev/null || { echo "mysql_config_editor not found in PATH" >&2; exit 1; }
command -v expect >/dev/null || { echo "expect not found in PATH (required for mysql_config_editor automation)" >&2; exit 1; }

projectDir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ -z "$CONFIG_PATH" ]]; then
  if [[ -f "${projectDir}/config/mysql/mysql-init.yaml" ]]; then
    CONFIG_PATH="${projectDir}/config/mysql/mysql-init.yaml"
  elif [[ -f "${projectDir}/config/mysql-init.yaml" ]]; then
    CONFIG_PATH="${projectDir}/config/mysql-init.yaml"
  else
    CONFIG_PATH="${projectDir}/config/mysql/mysql-init.yaml"
  fi
fi

if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "Config file not found: $CONFIG_PATH" >&2
  echo "Copy config/mysql/mysql-init.yaml.example to $CONFIG_PATH and fill values." >&2
  exit 1
fi

if [ ! -f "${projectDir}/config/schema_env.sh" ]; then
  echo "FATAL: config/schema_env.sh not found. Please run: scripts/gen_schema_env.sh" >&2
  exit 1
fi

# shellcheck disable=SC1091
source "${projectDir}/config/schema_env.sh"

DB_NAME="${OPS_META_DB:-$OPS_INSPECTION_DB}"

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

  # 输出当前记录：用 key=value 的形式，空格分隔
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

  # 跳过注释 / 空行 / 文档头 ---
  /^[ \t]*#/   { next }
  /^[ \t]*$/   { next }
  /^[ \t]*---/ { next }

  # 识别列表项开头：例如 "- type: mysql" 或 "- instance_name: xxx"
  /^[ \t]*-[ \t]*/ {
    # 新的一条记录开始，先把上一条 flush
    flush()
    line = $0
    sub(/^[ \t]*-[ \t]*/, "", line)
    line = trim(line)
    if (line == "") {
      record_started = 1
      next
    }
    # 如果这一行本身带 key: val，则继续按 key: val 解析
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

  # 普通的 key: val 行
  {
    # 如果之前还没有开始记录，则表明这是第一条记录（没有 - 开头）
    if (!record_started) {
      record_started = 1
    }
    line = $0
    # 去掉行首缩进
    sub(/^[ \t]+/, "", line)
    # 若找不到冒号，直接跳过（例如多行字符串的情况，这里我们不做处理）
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

records="$(parse_yaml "$CONFIG_PATH")"
login_path_rows=()

echo "Starting MySQL asset initialization from $CONFIG_PATH"
while IFS= read -r line; do
  # 先清空所有字段
  instance_name=""
  alias_name=""
  env=""
  host=""
  port=""
  username=""
  password=""
  login_path=""

  # 按 key=value 解析这一行
  for kv in $line; do
    key="${kv%%=*}"
    val="${kv#*=}"
    case "$key" in
      instance_name) instance_name="$val" ;;
      alias_name)    alias_name="$val" ;;
      env)           env="$val" ;;
      host)          host="$val" ;;
      port)          port="$val" ;;
      username)      username="$val" ;;
      password)      password="$val" ;;
      login_path)    login_path="$val" ;;
    esac
  done

  # 没有 instance_name，直接跳过
  if [[ -z "$instance_name" ]]; then
    continue
  fi

  # 默认值逻辑
  [[ -z "$port" ]] && port=3306          # port 不写 -> 3306
  # env 不写 -> NULL（变量为 ""，插入时写入 NULL）
  # alias_name / login_path 不写 -> 空（上面已经初始化为 ""）

  # 必须字段检查
  if [[ -z "$host" || -z "$username" || -z "$password" ]]; then
    echo "[SKIP] Missing required field for instance_name=$instance_name (need host/username/password)" >&2
    continue
  fi

  # env 校验：允许为空，或者在固定集合里
  case "$env" in
    ""|MOS|Purple|RTM|MIB2) ;;
    *)
      echo "[SKIP] Invalid env=$env for instance_name=$instance_name" >&2
      continue
      ;;
  esac

  alias_sql="NULL"
  [[ -n "$alias_name" ]] && alias_sql="'$(sql_escape "$alias_name")'"

  username_sql="NULL"
  [[ -n "$username" ]] && username_sql="'$(sql_escape "$username")'"

  login_path_sql="NULL"
  [[ -n "$login_path" ]] && login_path_sql="'$(sql_escape "$login_path")'"

  # env 为空时，插入 NULL；非空时插入具体枚举值
  env_sql="NULL"
  [[ -n "$env" ]] && env_sql="'$(sql_escape "$env")'"

  # 根据 env 是否为空构造查询条件（空 -> IS NULL）
  if [[ -z "$env" ]]; then
    env_condition="env IS NULL"
  else
    env_condition="env='$(sql_escape "$env")'"
  fi

  existing_row="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "
SELECT instance_id, COALESCE(login_path,'') FROM ${T_ASSET_INSTANCE}
WHERE type='mysql' AND ${env_condition}
  AND host='$(sql_escape "$host")'
  AND port=$port
  AND instance_name='$(sql_escape "$instance_name")'
LIMIT 1;
")"

  instance_id=""
  existing_login_path=""
  if [[ -n "$existing_row" ]]; then
    IFS=$'\t' read -r instance_id existing_login_path <<<"$existing_row"
    echo "[SKIP] Exists instance_name=$instance_name env=${env:-null} host=$host port=$port (instance_id=$instance_id, login_path=${existing_login_path:-null})"
    if [[ -z "$existing_login_path" ]]; then
      login_path_final="$login_path"
      if [[ -z "$login_path_final" ]]; then
        login_path_final="${instance_name}"
      fi
      mysql --login-path="$OPS_META_LOGIN_PATH" -D "$DB_NAME" -e "
UPDATE ${T_ASSET_INSTANCE} SET login_path='$(sql_escape "$login_path_final")'
WHERE instance_id=$instance_id;
"
      existing_login_path="$login_path_final"
      echo "[UPDATE] Set login_path=$login_path_final for instance_id=$instance_id"
    fi
  else
    insert_sql="
INSERT INTO ${T_ASSET_INSTANCE}(
  instance_name, alias_name, env, host, port, username, login_path
) VALUES (
  '$(sql_escape "$instance_name")',
  $alias_sql,
  $env_sql,
  '$(sql_escape "$host")',
  $port,
  $username_sql,
  $login_path_sql
);
SELECT LAST_INSERT_ID();
"
    new_id="$(mysql --login-path="$OPS_META_LOGIN_PATH" --batch --raw -N -D "$DB_NAME" -e "$insert_sql")"
    instance_id="$new_id"
    echo "[NEW] Inserted instance_name=$instance_name env=$env host=$host port=$port -> instance_id=$instance_id"
  fi

  # 处理 mysql_config_editor login-path
  login_path_final="$login_path"
  if [[ -z "$login_path_final" ]]; then
    if [[ -n "$existing_login_path" ]]; then
      login_path_final="$existing_login_path"
    else
      login_path_final="${instance_name}"
    fi
  fi

  if [[ -z "$login_path_final" ]]; then
    echo "WARN: skip asset env=${env:-} host=${host} port=${port} because login_path is empty" >&2
    continue
  fi

  login_path_rows+=("$instance_id"$'\t'"$host"$'\t'"$port"$'\t'"$username"$'\t'"$password"$'\t'"$login_path_final")

done <<< "$records"

# 为所有需要的实例创建 login-path
echo "Creating mysql_config_editor login-path entries..."
for row in "${login_path_rows[@]}"; do
  IFS=$'\t' read -r instance_id host port username password login_path_final <<<"$row"

  # strip accidental prefixes if present
  host="${host#host=}"
  port="${port#port=}"
  username="${username#username=}"
  login_path_final="${login_path_final#login_path=}"

  if [[ -z "$password" ]]; then
    echo "[WARN] Missing password for instance_id=$instance_id; skipping mysql_config_editor" >&2
    continue
  fi

  # standard mysql_config_editor set with clean parameters
  if ! create_login_path "$login_path_final" "$host" "$username" "$port" "$password"; then
    echo "[ERROR] Failed to create login_path=$login_path_final for instance_id=$instance_id" >&2
  else
    echo "[LOGIN_PATH] mysql_config_editor set --login-path=$login_path_final (host=$host port=$port user=$username)"
  fi

done

echo "Initialization completed."
