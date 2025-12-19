-- 兼容 MySQL 5.7/8.0，满足 ONLY_FULL_GROUP_BY；被巡检实例与 meta 库均可为 5.7/8.0

-- =========================================================
-- Q1: 失败实例列表（最新一次采集失败）
-- 输出：env, alias_name, instance_name, host, port, last_stat_time, logical_total_gb, mysql_version, last_collect_status, error_msg
-- =========================================================
SELECT
  CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
  a.alias_name,
  a.instance_name,
  a.host,
  a.port,
  s.stat_time AS last_stat_time,
  ROUND(s.logical_total_bytes / POW(1024, 3), 2) AS logical_total_gb,
  s.mysql_version,
  s.collect_status AS last_collect_status,
  s.error_msg
FROM ops_inspection.snap_mysql_instance_storage s
JOIN (
  SELECT instance_id, MAX(stat_time) AS stat_time
  FROM ops_inspection.snap_mysql_instance_storage
  GROUP BY instance_id
) ls ON s.instance_id = ls.instance_id AND s.stat_time = ls.stat_time
JOIN ops_inspection.asset_instance a
  ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND s.collect_status = 'failed'
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =========================================================
-- Q2: 按 env 聚合容量（最新 vs 上一次，仅成功实例）
-- 输出：env, instance_count, last_env_total_gb, prev_env_total_gb, diff_env_total_gb_fmt
-- =========================================================
SELECT
  env,
  COUNT(*) AS instance_count,
  ROUND(SUM(last_total_bytes) / POW(1024, 3), 2) AS last_env_total_gb,
  ROUND(SUM(prev_total_bytes) / POW(1024, 3), 2) AS prev_env_total_gb,
  CASE
    WHEN SUM(last_total_bytes - prev_total_bytes) > 0 THEN CONCAT('+', ROUND(SUM(last_total_bytes - prev_total_bytes) / POW(1024, 3), 2))
    WHEN SUM(last_total_bytes - prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(SUM(last_total_bytes - prev_total_bytes)) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_env_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    last_rec.instance_id,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    GROUP BY instance_id
  ) lt
  JOIN ops_inspection.snap_mysql_instance_storage last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ops_inspection.snap_mysql_instance_storage s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a
    ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
    AND last_rec.collect_status = 'ok'
) inst_ok
GROUP BY env
ORDER BY SUM(last_total_bytes - prev_total_bytes) DESC, last_env_total_gb DESC;

-- =========================================================
-- Q3: 实例维度最近 vs 上一次容量（含 data/index/total 差异，全部成功实例）
-- 输出：env, alias_name, instance_name, host, port, last_stat_time, prev_stat_time,
--       last_data_gb, prev_data_gb, diff_data_gb_fmt,
--       last_index_gb, prev_index_gb, diff_index_gb_fmt,
--       last_total_gb, prev_total_gb, diff_total_gb_fmt
-- =========================================================
SELECT
  io.env,
  io.alias_name,
  io.instance_name,
  io.host,
  io.port,
  io.last_stat_time,
  io.prev_stat_time,
  ROUND(io.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
  ROUND(io.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_data_bytes > 0 THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2))
    WHEN io.diff_data_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_index_bytes > 0 THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2))
    WHEN io.diff_index_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE
    WHEN io.prev_stat_time IS NULL THEN '-'
    WHEN io.diff_total_bytes > 0 THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2))
    WHEN io.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt
FROM (
  SELECT
    CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
    a.alias_name,
    a.instance_name,
    a.host,
    a.port,
    last_rec.stat_time AS last_stat_time,
    prev_rec.stat_time AS prev_stat_time,
    last_rec.logical_data_bytes AS last_data_bytes,
    IFNULL(prev_rec.logical_data_bytes, 0) AS prev_data_bytes,
    (last_rec.logical_data_bytes - IFNULL(prev_rec.logical_data_bytes, 0)) AS diff_data_bytes,
    last_rec.logical_index_bytes AS last_index_bytes,
    IFNULL(prev_rec.logical_index_bytes, 0) AS prev_index_bytes,
    (last_rec.logical_index_bytes - IFNULL(prev_rec.logical_index_bytes, 0)) AS diff_index_bytes,
    last_rec.logical_total_bytes AS last_total_bytes,
    IFNULL(prev_rec.logical_total_bytes, 0) AS prev_total_bytes,
    (last_rec.logical_total_bytes - IFNULL(prev_rec.logical_total_bytes, 0)) AS diff_total_bytes
  FROM (
    SELECT instance_id, MAX(stat_time) AS last_time
    FROM ops_inspection.snap_mysql_instance_storage
    GROUP BY instance_id
  ) lt
  JOIN ops_inspection.snap_mysql_instance_storage last_rec
    ON last_rec.instance_id = lt.instance_id AND last_rec.stat_time = lt.last_time
  LEFT JOIN (
    SELECT s.instance_id, MAX(s.stat_time) AS prev_time
    FROM ops_inspection.snap_mysql_instance_storage s
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_instance_storage
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a
    ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
    AND last_rec.collect_status = 'ok'
) io
ORDER BY ABS(io.diff_total_bytes) DESC, io.env, io.instance_name;

-- =========================================================
-- Q4: 库维度当前容量 Top5（每实例先取 Top5，再全局排名+排名变化，成功实例）
-- 输出：env, alias_name, instance_name, schema_name, last_total_gb, prev_total_gb, diff_total_gb_fmt,
--       last_rank_no_global, prev_rank_no_global, rank_delta, rank_delta_fmt
-- =========================================================
SELECT
  final.env,
  final.alias_name,
  final.instance_name,
  final.schema_name,
  final.last_total_gb,
  final.prev_total_gb,
  CASE
    WHEN final.prev_total_bytes IS NULL THEN '-'
    WHEN final.diff_total_bytes > 0 THEN CONCAT('+', ROUND(final.diff_total_bytes / POW(1024, 3), 2))
    WHEN final.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(final.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt,
  CASE
    WHEN final.prev_rank_no_global IS NULL THEN '-'
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) > 0 THEN CONCAT('+', (final.prev_rank_no_global - final.last_rank_no_global))
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) < 0 THEN CONCAT('-', ABS(final.prev_rank_no_global - final.last_rank_no_global))
    ELSE '0'
  END AS rank_delta_fmt
FROM (
  SELECT
    top5.env,
    top5.alias_name,
    top5.instance_name,
    top5.schema_name,
    ROUND(top5.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(top5.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    top5.last_total_bytes,
    top5.prev_total_bytes,
    (top5.last_total_bytes - IFNULL(top5.prev_total_bytes, 0)) AS diff_total_bytes,
    lr.last_rank_no_global,
    pr.prev_rank_no_global
  FROM (
    SELECT
      ranked_inst.env,
      ranked_inst.alias_name,
      ranked_inst.instance_name,
      ranked_inst.instance_id,
      ranked_inst.schema_name,
      ranked_inst.last_total_bytes,
      ranked_inst.prev_total_bytes
    FROM (
      SELECT
        base.env,
        base.alias_name,
        base.instance_name,
        base.instance_id,
        base.schema_name,
        base.last_total_bytes,
        base.prev_total_bytes,
        @row_inst := IF(@cur_inst = base.instance_id, @row_inst + 1, 1) AS row_in_inst,
        @cur_inst := base.instance_id AS cur_inst
      FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.last_total_bytes,
          p.prev_total_bytes
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            SUM(t.total_bytes) AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
          GROUP BY lt.instance_id, t.schema_name
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            SUM(t.total_bytes) AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
          GROUP BY pt.instance_id, t.schema_name
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name
      ) base
      JOIN (SELECT @row_inst := 0, @cur_inst := NULL) vars
      ORDER BY base.instance_id, base.last_total_bytes DESC, base.schema_name
    ) ranked_inst
    WHERE ranked_inst.row_in_inst <= 5
  ) top5
  JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      @g_last := @g_last + 1 AS last_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.last_total_bytes,
          p.prev_total_bytes,
          @row_inst2 := IF(@cur_inst2 = l.instance_id, @row_inst2 + 1, 1) AS row_in_inst2,
          @cur_inst2 := l.instance_id AS cur_inst2
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            SUM(t.total_bytes) AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
          GROUP BY lt.instance_id, t.schema_name
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            SUM(t.total_bytes) AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
          GROUP BY pt.instance_id, t.schema_name
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name
        JOIN (SELECT @row_inst2 := 0, @cur_inst2 := NULL) vrs
        ORDER BY l.instance_id, l.last_total_bytes DESC, l.schema_name
      ) tmp WHERE row_in_inst2 <= 5
    ) t
    JOIN (SELECT @g_last := 0) gl
    ORDER BY t.last_total_bytes DESC, t.schema_name, t.instance_id
  ) lr ON lr.instance_id = top5.instance_id AND lr.schema_name = top5.schema_name
  LEFT JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      @g_prev := @g_prev + 1 AS prev_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          l.instance_id,
          l.schema_name,
          l.prev_total_bytes,
          @row_inst3 := IF(@cur_inst3 = l.instance_id, @row_inst3 + 1, 1) AS row_in_inst3,
          @cur_inst3 := l.instance_id AS cur_inst3
        FROM (
          SELECT
            l.instance_id,
            t.schema_name,
            SUM(t.total_bytes) AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) l
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = l.instance_id AND t.stat_time = l.prev_time
          GROUP BY l.instance_id, t.schema_name
        ) l
        JOIN (SELECT @row_inst3 := 0, @cur_inst3 := NULL) v3
        ORDER BY l.instance_id, l.prev_total_bytes DESC, l.schema_name
      ) tmp2 WHERE row_in_inst3 <= 5
    ) t
    JOIN (SELECT @g_prev := 0) gp
    ORDER BY (t.prev_total_bytes IS NULL), t.prev_total_bytes DESC, t.schema_name, t.instance_id
  ) pr ON pr.instance_id = top5.instance_id AND pr.schema_name = top5.schema_name
) final
ORDER BY final.last_rank_no_global;

-- =========================================================
-- Q5: 表维度当前容量 Top10（每实例先取 Top10，再全局排名+排名变化，成功实例）
-- 输出：env, alias_name, instance_name, schema_name, table_name, last_total_gb, prev_total_gb, diff_total_gb_fmt,
--       last_rank_no_global, prev_rank_no_global, rank_delta, rank_delta_fmt
-- =========================================================
SELECT
  final.env,
  final.alias_name,
  final.instance_name,
  final.schema_name,
  final.table_name,
  final.last_total_gb,
  final.prev_total_gb,
  CASE
    WHEN final.prev_total_bytes IS NULL THEN '-'
    WHEN final.diff_total_bytes > 0 THEN CONCAT('+', ROUND(final.diff_total_bytes / POW(1024, 3), 2))
    WHEN final.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(final.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt,
  CASE
    WHEN final.prev_rank_no_global IS NULL THEN '-'
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) > 0 THEN CONCAT('+', (final.prev_rank_no_global - final.last_rank_no_global))
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) < 0 THEN CONCAT('-', ABS(final.prev_rank_no_global - final.last_rank_no_global))
    ELSE '0'
  END AS rank_delta_fmt
FROM (
  SELECT
    top10.env,
    top10.alias_name,
    top10.instance_name,
    top10.schema_name,
    top10.table_name,
    ROUND(top10.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(top10.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    top10.last_total_bytes,
    top10.prev_total_bytes,
    (top10.last_total_bytes - IFNULL(top10.prev_total_bytes, 0)) AS diff_total_bytes,
    lr.last_rank_no_global,
    pr.prev_rank_no_global
  FROM (
    SELECT
      ranked_inst.env,
      ranked_inst.alias_name,
      ranked_inst.instance_name,
      ranked_inst.instance_id,
      ranked_inst.schema_name,
      ranked_inst.table_name,
      ranked_inst.last_total_bytes,
      ranked_inst.prev_total_bytes
    FROM (
      SELECT
        base.env,
        base.alias_name,
        base.instance_name,
        base.instance_id,
        base.schema_name,
        base.table_name,
        base.last_total_bytes,
        base.prev_total_bytes,
        @row_inst := IF(@cur_inst = base.instance_id, @row_inst + 1, 1) AS row_in_inst,
        @cur_inst := base.instance_id AS cur_inst
      FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.last_total_bytes,
          p.prev_total_bytes
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
      ) base
      JOIN (SELECT @row_inst := 0, @cur_inst := NULL) vars
      ORDER BY base.instance_id, base.last_total_bytes DESC, base.schema_name, base.table_name
    ) ranked_inst
    WHERE ranked_inst.row_in_inst <= 10
  ) top10
  JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      t.table_name,
      @g_last := @g_last + 1 AS last_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.last_total_bytes,
          p.prev_total_bytes,
          @row_inst2 := IF(@cur_inst2 = l.instance_id, @row_inst2 + 1, 1) AS row_in_inst2,
          @cur_inst2 := l.instance_id AS cur_inst2
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
        JOIN (SELECT @row_inst2 := 0, @cur_inst2 := NULL) vrs
        ORDER BY l.instance_id, l.last_total_bytes DESC, l.schema_name, l.table_name
      ) tmp WHERE row_in_inst2 <= 10
    ) t
    JOIN (SELECT @g_last := 0) gl
    ORDER BY t.last_total_bytes DESC, t.schema_name, t.table_name, t.instance_id
  ) lr ON lr.instance_id = top10.instance_id AND lr.schema_name = top10.schema_name AND lr.table_name = top10.table_name
  LEFT JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      t.table_name,
      @g_prev := @g_prev + 1 AS prev_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.prev_total_bytes,
          @row_inst3 := IF(@cur_inst3 = l.instance_id, @row_inst3 + 1, 1) AS row_in_inst3,
          @cur_inst3 := l.instance_id AS cur_inst3
        FROM (
          SELECT
            l.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) l
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = l.instance_id AND t.stat_time = l.prev_time
        ) l
        JOIN (SELECT @row_inst3 := 0, @cur_inst3 := NULL) v3
        ORDER BY l.instance_id, l.prev_total_bytes DESC, l.schema_name, l.table_name
      ) tmp2 WHERE row_in_inst3 <= 10
    ) t
    JOIN (SELECT @g_prev := 0) gp
    ORDER BY (t.prev_total_bytes IS NULL), t.prev_total_bytes DESC, t.schema_name, t.table_name, t.instance_id
  ) pr ON pr.instance_id = top10.instance_id AND pr.schema_name = top10.schema_name AND pr.table_name = top10.table_name
) final
ORDER BY final.last_rank_no_global;

-- =========================================================
-- Q6: 表维度近两次容量差异 Top10（每实例先取 Top10 by |diff|，再全局排名+排名变化，成功实例）
-- 输出：env, alias_name, instance_name, schema_name, table_name, last_total_gb, prev_total_gb, diff_total_gb_fmt,
--       last_rank_no_global, prev_rank_no_global, rank_delta, rank_delta_fmt
-- =========================================================
SELECT
  final.env,
  final.alias_name,
  final.instance_name,
  final.schema_name,
  final.table_name,
  final.last_total_gb,
  final.prev_total_gb,
  CASE
    WHEN final.prev_total_bytes IS NULL THEN '-'
    WHEN final.diff_total_bytes > 0 THEN CONCAT('+', ROUND(final.diff_total_bytes / POW(1024, 3), 2))
    WHEN final.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(final.diff_total_bytes) / POW(1024, 3), 2))
    ELSE '0'
  END AS diff_total_gb_fmt,
  CASE
    WHEN final.prev_rank_no_global IS NULL THEN '-'
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) > 0 THEN CONCAT('+', (final.prev_rank_no_global - final.last_rank_no_global))
    WHEN (final.prev_rank_no_global - final.last_rank_no_global) < 0 THEN CONCAT('-', ABS(final.prev_rank_no_global - final.last_rank_no_global))
    ELSE '0'
  END AS rank_delta_fmt
FROM (
  SELECT
    top10.env,
    top10.alias_name,
    top10.instance_name,
    top10.schema_name,
    top10.table_name,
    ROUND(top10.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(top10.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    top10.last_total_bytes,
    top10.prev_total_bytes,
    (top10.last_total_bytes - IFNULL(top10.prev_total_bytes, 0)) AS diff_total_bytes,
    lr.last_rank_no_global,
    pr.prev_rank_no_global
  FROM (
    SELECT
      ranked_inst.env,
      ranked_inst.alias_name,
      ranked_inst.instance_name,
      ranked_inst.instance_id,
      ranked_inst.schema_name,
      ranked_inst.table_name,
      ranked_inst.last_total_bytes,
      ranked_inst.prev_total_bytes
    FROM (
      SELECT
        base.env,
        base.alias_name,
        base.instance_name,
        base.instance_id,
        base.schema_name,
        base.table_name,
        base.last_total_bytes,
        base.prev_total_bytes,
        @row_inst := IF(@cur_inst = base.instance_id, @row_inst + 1, 1) AS row_in_inst,
        @cur_inst := base.instance_id AS cur_inst
      FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.last_total_bytes,
          p.prev_total_bytes
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
      ) base
      JOIN (SELECT @row_inst := 0, @cur_inst := NULL) vars
      ORDER BY base.instance_id, ABS(base.last_total_bytes - IFNULL(base.prev_total_bytes, 0)) DESC, base.schema_name, base.table_name
    ) ranked_inst
    WHERE ranked_inst.row_in_inst <= 10
  ) top10
  JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      t.table_name,
      @g_last := @g_last + 1 AS last_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          si.env,
          si.alias_name,
          si.instance_name,
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.last_total_bytes,
          p.prev_total_bytes,
          @row_inst2 := IF(@cur_inst2 = l.instance_id, @row_inst2 + 1, 1) AS row_in_inst2,
          @cur_inst2 := l.instance_id AS cur_inst2
        FROM (
          SELECT
            CAST(a.instance_id AS CHAR) AS instance_id,
            CASE WHEN a.env IS NULL OR a.env = '' THEN '-' ELSE a.env END AS env,
            a.alias_name,
            a.instance_name
          FROM ops_inspection.asset_instance a
          JOIN (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_instance_storage
            GROUP BY instance_id
          ) ilt ON CAST(ilt.instance_id AS CHAR) = a.instance_id
          JOIN ops_inspection.snap_mysql_instance_storage ilast
            ON ilast.instance_id = ilt.instance_id AND ilast.stat_time = ilt.last_time
          WHERE a.is_active = 1 AND ilast.collect_status = 'ok'
        ) si
        JOIN (
          SELECT
            lt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
        ) l ON l.instance_id = si.instance_id
        LEFT JOIN (
          SELECT
            pt.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
        ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
        JOIN (SELECT @row_inst2 := 0, @cur_inst2 := NULL) vrs
        ORDER BY l.instance_id, ABS(l.total_bytes - IFNULL(p.prev_total_bytes, 0)) DESC, l.schema_name, l.table_name
      ) tmp WHERE row_in_inst2 <= 10
    ) t
    JOIN (SELECT @g_last := 0) gl
    ORDER BY ABS(t.last_total_bytes - IFNULL(t.prev_total_bytes, 0)) DESC, t.schema_name, t.table_name, t.instance_id
  ) lr ON lr.instance_id = top10.instance_id AND lr.schema_name = top10.schema_name AND lr.table_name = top10.table_name
  LEFT JOIN (
    SELECT
      t.instance_id,
      t.schema_name,
      t.table_name,
      @g_prev := @g_prev + 1 AS prev_rank_no_global
    FROM (
      SELECT * FROM (
        SELECT
          l.instance_id,
          l.schema_name,
          l.table_name,
          l.prev_total_bytes,
          @row_inst3 := IF(@cur_inst3 = l.instance_id, @row_inst3 + 1, 1) AS row_in_inst3,
          @cur_inst3 := l.instance_id AS cur_inst3
        FROM (
          SELECT
            l.instance_id,
            t.schema_name,
            t.table_name,
            t.total_bytes AS prev_total_bytes
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) l
          JOIN ops_inspection.snap_mysql_table_topn t
            ON t.instance_id = l.instance_id AND t.stat_time = l.prev_time
        ) l
        JOIN (SELECT @row_inst3 := 0, @cur_inst3 := NULL) v3
        ORDER BY l.instance_id, l.prev_total_bytes DESC, l.schema_name, l.table_name
      ) tmp2 WHERE row_in_inst3 <= 10
    ) t
    JOIN (SELECT @g_prev := 0) gp
    ORDER BY (t.prev_total_bytes IS NULL), t.prev_total_bytes DESC, t.schema_name, t.table_name, t.instance_id
  ) pr ON pr.instance_id = top10.instance_id AND pr.schema_name = top10.schema_name AND pr.table_name = top10.table_name
) final
ORDER BY final.last_rank_no_global;
