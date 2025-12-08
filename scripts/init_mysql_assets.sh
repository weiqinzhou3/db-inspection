#!/usr/bin/env bash
set -euo pipefail

# Initialize MySQL assets into ops_inspection.asset_instance and create login-path entries.
# Requirements:
#   - mysql client and mysql_config_editor available
#   - python3 with PyYAML (pip install pyyaml) to parse config/mysql-init.yaml
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

sql_escape() {
  # Escape single quotes for SQL literals
  local s="${1//\'/\'\'}"
  printf "%s" "$s"
}

parse_yaml() {
  python3 - "$CONFIG_PATH" <<'PY'
import sys, json
from pathlib import Path
try:
    import yaml  # type: ignore
except ImportError:
    sys.stderr.write("PyYAML is required. Install via: pip install pyyaml\n")
    sys.exit(1)

path = Path(sys.argv[1])
data = yaml.safe_load(path.read_text())
if not isinstance(data, list):
    sys.stderr.write("Config must be a YAML list of asset objects\n")
    sys.exit(1)

allowed_env = {"MOS", "Purple", "RTM", "MIB2"}
for idx, item in enumerate(data, 1):
    if not isinstance(item, dict):
        sys.stderr.write(f"Item #{idx} is not a mapping\n")
        sys.exit(1)
    for key in ("instance_name", "env", "host", "port", "username", "password"):
        if not item.get(key):
            sys.stderr.write(f"Missing required field '{key}' in item #{idx}\n")
            sys.exit(1)
    if item["env"] not in allowed_env:
        sys.stderr.write(f"Invalid env '{item['env']}' in item #{idx}\n")
        sys.exit(1)
    fields = [
        str(item.get("instance_name", "")),
        str(item.get("alias_name", "")) if item.get("alias_name") is not None else "",
        str(item.get("env", "")),
        str(item.get("host", "")),
        str(item.get("port", "")),
        str(item.get("username", "")),
        str(item.get("password", "")),
        str(item.get("login_path", "")) if item.get("login_path") is not None else "",
    ]
    print("\t".join(fields))
PY
}

records="$(parse_yaml)"

echo "Starting MySQL asset initialization from $CONFIG_PATH"
while IFS=$'\t' read -r instance_name alias_name env host port username password login_path; do
  if [[ -z "$instance_name" ]]; then
    continue
  fi
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
    # Backfill login_path if missing and config provides one
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

  # Determine final login_path value
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

  printf '%s\n' "$password" | mysql_config_editor set --login-path="$login_path_final" --host="$host" --user="$username" --port="$port" --password --skip-warn >/dev/null
  echo "[LOGIN_PATH] mysql_config_editor set --login-path=$login_path_final (host=$host port=$port user=$username)"

done <<< "$records"

echo "Initialization completed."
