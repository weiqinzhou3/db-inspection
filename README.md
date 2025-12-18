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

`sql/analysis.sql` provides Q1~Q6 analysis views (compatible with MySQL 5.7/8 and ONLY_FULL_GROUP_BY):
- Q1: Failed instances (latest snapshot)
- Q2: Env summary (latest vs previous diff)
- Q3: Top20 instances by diff (latest vs previous)
- Q4: All instances latest vs previous detail
- Q5: Latest capacity overview per instance
- Q6: Top20 tables rank change (latest vs previous round)

Fields like `diff_*_fmt` and `rank_delta_fmt` are designed for report/email highlighting.

Run analysis SQL against the meta DB:

- `mysql --login-path=ops_meta -D ops_inspection < sql/analysis.sql`
- Or `OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/run_mysql_analysis.sh`

## Export analysis TSV

Export Q1~Q6 results to `out/mysql_analysis/q*.tsv` for downstream reporting:

- `OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/export_mysql_analysis_tsv.sh`

Files generated:
- `out/mysql_analysis/q1_failed_instances.tsv`
- `out/mysql_analysis/q2_env_summary.tsv`
- `out/mysql_analysis/q3_instance_diff_top20.tsv`
- `out/mysql_analysis/q4_instance_last_vs_prev.tsv`
- `out/mysql_analysis/q5_instance_latest_capacity.tsv`
- `out/mysql_analysis/q6_table_top20_rank_change.tsv`

Email delivery is handled by `scripts/post_mysql_analysis_mail.sh`, which reads the TSV files and builds HTML; no additional mail-sending logic is needed here.
