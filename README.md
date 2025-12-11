# DB Inspection V1

## MySQL asset initialization (login-path based)

1) Create meta schema/tables:  
`mysql --login-path=ops_meta < sql/ddl.sql`

2) Prepare config (not committed):  
`cp config/mysql-init.yaml.example config/mysql-init.yaml` then fill fields. Password is only used locally to create `login_path`, never stored in DB.

3) Import assets and create login-path entries:  
`OPS_META_LOGIN_PATH=ops_meta ./scripts/init_mysql_assets.sh`

4) Verify collection/load pipeline against one target instance:  
`TARGET_LOGIN_PATH=<target_lp> OPS_META_LOGIN_PATH=ops_meta INSTANCE_ID=<id> scripts/examples/two_connection_demo.sh`

## Batch MySQL inspection

Run across all active MySQL assets (type=mysql, is_active=1, auth_mode=login_path) and load into meta DB:

`OPS_META_LOGIN_PATH=ops_meta ./scripts/run_mysql_inspection.sh`

## Analysis queries

Run analysis SQL against the meta DB (compatible with MySQL 5.7/8):

- `mysql --login-path=ops_meta -D ops_inspection < sql/analysis.sql`
- Or `OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/run_mysql_analysis.sh`
