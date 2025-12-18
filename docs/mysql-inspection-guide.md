# MySQL Inspection Guide (Step-by-Step)

This guide walks you through running the MySQL inspection end to end.

## 1) Prerequisites

Install the following on the inspection machine:

- MySQL client (`mysql`)
- Python 3 + pip (for PyYAML used by `scripts/init_mysql_assets.sh`)
- `sendEmail` (or an equivalent CLI mail tool) available in `PATH`

Configure a login-path for the meta DB:

```
mysql_config_editor set --login-path=ops_meta --host=<meta_host> --user=<meta_user> --password
```

## 2) Clone the repository

```
git clone https://github.com/weiqinzhou3/db-inspection
cd db_inspection
```

## 3) Initialize meta DB schema (one-time)

Create `ops_inspection` schema and tables using `sql/ddl.sql`:

```
mysql --login-path=ops_meta -e "SOURCE sql/ddl.sql"
```

## 4) Configure MySQL assets to inspect

Copy the config template:

```
cp config/mysql-init.yaml.example config/mysql-init.yaml
```

Edit `config/mysql-init.yaml` fields:

- `env`: environment name (e.g., prod / staging)
- `alias_name`: friendly alias for reports
- `instance_name`: unique instance name
- `host` / `port`: MySQL endpoint
- `user` / `password`: used only to create local `login_path`
- `login_path`: optional; auto-generated if omitted

Notes:

- Passwords are **not** stored in the database. They are only used locally to create `login_path` entries.

Run the init script:

```
OPS_META_LOGIN_PATH=ops_meta ./scripts/init_mysql_assets.sh
```

This script will:

- insert assets into `ops_inspection.asset_instance`
- create `login_path` entries for each instance (if needed)

## 5) Run a full MySQL inspection

The inspection script:

- loops through all `is_active=1` assets
- collects instance summary and Top20 tables
- stores results in `snap_*` tables with `collect_status` and `error_msg`

Run:

```
OPS_META_LOGIN_PATH=ops_meta ./scripts/run_mysql_inspection.sh
```

## 6) View analysis results in terminal

Q1~Q6 views in `sql/analysis.sql`:

- Q1: failed instances (latest snapshot)
- Q2: env summary (latest vs previous)
- Q3: top20 instances by diff
- Q4: all instances latest vs previous detail
- Q5: latest capacity overview
- Q6: top20 table rank change

All capacity values are in **GB** (rounded to 2 decimals).

Run:

```
mysql --login-path=ops_meta -D ops_inspection < sql/analysis.sql
```

## 7) Export TSV analysis results

This exports Q1~Q6 to TSV files under `out/mysql_analysis/`:

```
OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/export_mysql_analysis_tsv.sh
```

Files generated:

- `q1_failed_instances.tsv`
- `q2_env_summary.tsv`
- `q3_instance_diff_top20.tsv`
- `q4_instance_last_vs_prev.tsv`
- `q5_instance_latest_capacity.tsv`
- `q6_table_top20_rank_change.tsv`

All capacity values in these TSVs are in **GB**.

## 8) Build and send the email report

The mail script:

- reads the 6 TSV files
- builds an HTML report (gray header, bold titles, red highlights)
- sends via `sendEmail`

Environment variables used:

- `EMAIL_RECIVER`, `EMAIL_SENDER`, `EMAIL_USERNAME`
- `EMAIL_PASSWORD`, `EMAIL_SMTPHOST`

Run:

```
./scripts/post_mysql_analysis_mail.sh
```

Default subject is `[Report] MySQL Inspection Summary`. The body includes Q1~Q6 sections with a description and a table.

## 9) (Optional) Schedule daily runs

Example crontab (daily at 09:00):

```
0 9 * * * cd /path/to/db_inspection && \
  OPS_META_LOGIN_PATH=ops_meta ./scripts/run_mysql_inspection.sh && \
  OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection ./scripts/export_mysql_analysis_tsv.sh && \
  ./scripts/post_mysql_analysis_mail.sh
```
