-- Insert template executed on ops_inspection meta database
-- Replace {{...}} placeholders with collected values before execution.
-- Supply string placeholders as quoted literals or NULL when absent.
INSERT INTO ops_inspection.snap_mysql_instance_storage (
  stat_time,
  instance_id,
  logical_data_bytes,
  logical_index_bytes,
  logical_total_bytes,
  mysql_version,
  collect_status,
  error_msg
) VALUES (
  '{{stat_time}}',
  '{{instance_id}}',
  {{logical_data_bytes}},
  {{logical_index_bytes}},
  {{logical_total_bytes}},
  '{{mysql_version}}',
  'ok',
  NULL
);
