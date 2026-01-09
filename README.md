# DB Inspection V1

## MySQL asset initialization (login-path based)

1) Create meta schema/tables:  
`mysql --login-path=ops_meta < sql/ddl.sql`

2) Prepare config (not committed):  
`cp config/mysql/mysql-init.yaml.example config/mysql/mysql-init.yaml` then fill fields. Password is only used locally to create `login_path`, never stored in DB.

3) Import assets and create login-path entries:  
`OPS_META_LOGIN_PATH=ops_meta ./scripts/mysql/init_mysql_assets.sh`

4) Verify collection/load pipeline against one target instance:  
`TARGET_LOGIN_PATH=<target_lp> OPS_META_LOGIN_PATH=ops_meta INSTANCE_ID=<id> scripts/mysql/examples/two_connection_demo.sh`

## Batch MySQL inspection

Run across all active MySQL assets (type=mysql, is_active=1, auth_mode=login_path) and load into meta DB:

`OPS_META_LOGIN_PATH=ops_meta ./scripts/mysql/run_mysql_inspection.sh`

## Schema/table mapping

Logical names for schema and tables are declared in `sql/ddl.sql` via `-- @schema` / `-- @table` annotations. Generate the runtime mapping file with:

`./scripts/gen_schema_env.sh`

All shell scripts source `config/schema_env.sh` and reference schema/table names via variables. To change schema/table names in the future:

1) Update the `@schema` / `@table` annotations in `sql/ddl.sql`  
2) Run `./scripts/gen_schema_env.sh`  
3) No shell script edits required

## Requirements

- MySQL: `requirements/mysql.txt`
- MongoDB: `requirements/mongo.txt`

## Analysis queries

`sql/analysis.sql` provides Q1~Q6 analysis views (compatible with MySQL 5.7/8 and ONLY_FULL_GROUP_BY, all capacity outputs in GB):
- Q1: Failed instances (latest failed snapshot)
- Q2: Env summary (latest vs previous, only latest-success instances; diff in fmt only)
- Q3: Instance latest vs previous (data/index/total with fmt diffs)
- Q4: Schema (per instance) current capacity Top5
- Q5: Table (per instance) current capacity Top10
- Q6: Table (per instance) last vs prev capacity diff Top10

All capacity numbers in Q1~Q6 are output in GB (rounded to 2 decimals). Fields like `diff_*_fmt` and `rank_delta_fmt` are designed for report/email highlighting.

Run analysis SQL against the meta DB:

- `mysql --login-path=ops_meta -D ops_inspection < sql/analysis.sql`
- Or `OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/mysql/run_mysql_analysis.sh`

## Export analysis TSV

Export Q1~Q5 results to `out/mysql/analysis/q*.tsv` for downstream reporting (capacity values in GB):

- `OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/mysql/export_mysql_analysis_tsv.sh`

Files generated:
- `out/mysql/analysis/q1_failed_instances.tsv`
- `out/mysql/analysis/q2_env_summary.tsv`
- `out/mysql/analysis/q3_instance_last_vs_prev.tsv`
- `out/mysql/analysis/q4_table_last.tsv`
- `out/mysql/analysis/q5_table_diff.tsv`

Email delivery is handled by `scripts/mysql/post_mysql_analysis_mail.sh`, which reads the TSV files and builds HTML; no additional mail-sending logic is needed here.

## Mail configuration

Provide mail settings via environment variables or `config/mail_env.sh` (create from `config/mail_env.example.sh`):

- `EMAIL_RECIVER`, `EMAIL_SENDER`, `EMAIL_USERNAME`, `EMAIL_PASSWORD`, `EMAIL_SMTPHOST` (default smtp.qq.com), `EMAIL_TITLE` (default `[DB Inspection] MySQL Inspection Summary`)

Example:

```
OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/mysql/export_mysql_analysis_tsv.sh
./scripts/mysql/post_mysql_analysis_mail.sh
```
