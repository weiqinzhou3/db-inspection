-- Insert template executed on ops_inspection meta database
-- Replace {{...}} placeholders with collected values before execution.
-- Supply string placeholders as quoted literals (e.g., 'InnoDB') or NULL when absent.
INSERT INTO ops_inspection.snap_mysql_table_topn (
  stat_time,
  instance_id,
  schema_name,
  table_name,
  engine,
  table_rows,
  data_bytes,
  index_bytes,
  total_bytes,
  rank_no
) VALUES (
  '{{stat_time}}',
  '{{instance_id}}',
  '{{schema_name}}',
  '{{table_name}}',
  '{{engine}}',
  {{table_rows}},
  {{data_bytes}},
  {{index_bytes}},
  {{total_bytes}},
  {{rank_no}}
);
