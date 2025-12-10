#!/usr/bin/env bash
set -euo pipefail

# Usage: TARGET_LOGIN_PATH=target1 OPS_META_LOGIN_PATH=ops_meta INSTANCE_ID=101 ./two_connection_demo.sh
TARGET_LOGIN_PATH="${TARGET_LOGIN_PATH:-}"
OPS_META_LOGIN_PATH="${OPS_META_LOGIN_PATH:-}"
INSTANCE_ID="${INSTANCE_ID:-}"

if [[ -z "$TARGET_LOGIN_PATH" || -z "$OPS_META_LOGIN_PATH" || -z "$INSTANCE_ID" ]]; then
  echo "Usage: TARGET_LOGIN_PATH=target1 OPS_META_LOGIN_PATH=ops_meta INSTANCE_ID=101 $0" >&2
  exit 1
fi

tmp_dir="$(mktemp -d)"
summary_tsv="$tmp_dir/instance_summary.tsv"
topn_tsv="$tmp_dir/top20_tables.tsv"

cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Collect from inspected instance (read-only)
mysql --login-path="$TARGET_LOGIN_PATH" --batch --raw -N -e <<SQL 2>&1
  SET @instance_id:=$instance_id;
  SOURCE $root_dir/sql/collect/instance_logical_summary.sql
SQL
> "$summary_tsv"
mysql --login-path="$TARGET_LOGIN_PATH" --batch --raw -N -e <<SQL 2>&1
  SET @instance_id:=$instance_id;
  SOURCE $root_dir/sql/collect/top20_tables.sql
SQL
> "$topn_tsv"

# Load instance summary into meta DB
IFS=$'\t' read -r stat_time instance_id logical_data logical_index logical_total mysql_version < "$summary_tsv"
mysql --login-path="$OPS_META_LOGIN_PATH" -D ops_inspection -e "
INSERT INTO snap_mysql_instance_storage(stat_time,instance_id,logical_data_bytes,logical_index_bytes,logical_total_bytes,mysql_version,collect_status,error_msg)
VALUES ('$stat_time','$instance_id',$logical_data,$logical_index,$logical_total,'$mysql_version','ok',NULL);
"

# Load Top20 rows into meta DB
while IFS=$'\t' read -r stat_time instance_id schema_name table_name engine table_rows data_bytes index_bytes total_bytes rank_no; do
  engine_val="NULL"
  [[ -n "$engine" ]] && engine_val="'$engine'"
  table_rows_val="NULL"
  [[ -n "$table_rows" ]] && table_rows_val="$table_rows"

  mysql --login-path="$OPS_META_LOGIN_PATH" -D ops_inspection -e "
  INSERT INTO snap_mysql_table_topn(
    stat_time,instance_id,schema_name,table_name,engine,table_rows,data_bytes,index_bytes,total_bytes,rank_no
  ) VALUES (
    '$stat_time','$instance_id','$schema_name','$table_name',$engine_val,$table_rows_val,$data_bytes,$index_bytes,$total_bytes,$rank_no
  );
  "
done < "$topn_tsv"

echo "Done. Loaded into ops_inspection via $OPS_META_LOGIN_PATH"
