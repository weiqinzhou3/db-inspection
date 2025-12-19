-- 兼容 MySQL 5.7/8.0，满足 ONLY_FULL_GROUP_BY；被巡检实例与 meta 库均可为 5.7/8.0

-- =============================
-- Q1: 最新失败实例
-- =============================
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
JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = s.instance_id
WHERE a.is_active = 1
  AND s.collect_status = 'failed'
ORDER BY s.stat_time DESC, a.env, a.instance_name;

-- =============================
-- Q2: 按 env 聚合（仅最新成功实例）
-- =============================
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
    WHERE collect_status = 'ok'
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
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time AND s.collect_status = 'ok'
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
) inst_ok
GROUP BY env
ORDER BY SUM(last_total_bytes - prev_total_bytes) DESC, last_env_total_gb DESC;

-- =============================
-- Q3: 实例最近 vs 上一次容量（成功实例）
-- =============================
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
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_data_bytes > 0 THEN CONCAT('+', ROUND(io.diff_data_bytes / POW(1024, 3), 2)) WHEN io.diff_data_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_data_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_data_gb_fmt,
  ROUND(io.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
  ROUND(io.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_index_bytes > 0 THEN CONCAT('+', ROUND(io.diff_index_bytes / POW(1024, 3), 2)) WHEN io.diff_index_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_index_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_index_gb_fmt,
  ROUND(io.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
  ROUND(io.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
  CASE WHEN io.prev_stat_time IS NULL THEN '-' WHEN io.diff_total_bytes > 0 THEN CONCAT('+', ROUND(io.diff_total_bytes / POW(1024, 3), 2)) WHEN io.diff_total_bytes < 0 THEN CONCAT('-', ROUND(ABS(io.diff_total_bytes) / POW(1024, 3), 2)) ELSE '0' END AS diff_total_gb_fmt
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
    WHERE collect_status = 'ok'
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
      WHERE collect_status = 'ok'
      GROUP BY instance_id
    ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time AND s.collect_status = 'ok'
    GROUP BY s.instance_id
  ) pt ON pt.instance_id = lt.instance_id
  LEFT JOIN ops_inspection.snap_mysql_instance_storage prev_rec
    ON prev_rec.instance_id = pt.instance_id AND prev_rec.stat_time = pt.prev_time
  JOIN ops_inspection.asset_instance a ON CAST(a.instance_id AS CHAR) = lt.instance_id
  WHERE a.is_active = 1
) io
ORDER BY ABS(io.diff_total_bytes) DESC, io.env, io.instance_name;

-- =============================
-- Q4: schema 维度当前容量 Top5（每实例先取 Top5，再全局排序）
-- =============================
SELECT
  res.env,
  res.alias_name,
  res.instance_name,
  res.schema_name,
  res.last_data_gb,
  res.last_index_gb,
  res.last_total_gb,
  res.prev_data_gb,
  res.prev_index_gb,
  res.prev_total_gb,
  res.diff_total_gb_fmt
FROM (
  SELECT
    b.env,
    b.alias_name,
    b.instance_name,
    b.schema_name,
    ROUND(b.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
    ROUND(b.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
    ROUND(b.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(b.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
    ROUND(b.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
    ROUND(b.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    CASE
      WHEN b.prev_total_bytes IS NULL THEN '-'
      WHEN (b.last_total_bytes - b.prev_total_bytes) > 0 THEN CONCAT('+', ROUND((b.last_total_bytes - b.prev_total_bytes) / POW(1024, 3), 2))
      WHEN (b.last_total_bytes - b.prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(b.last_total_bytes - b.prev_total_bytes) / POW(1024, 3), 2))
      ELSE '0'
    END AS diff_total_gb_fmt
  FROM (
    SELECT
      io.env,
      io.alias_name,
      io.instance_name,
      io.instance_id,
      l.schema_name,
      l.last_data_bytes,
      l.last_index_bytes,
      l.last_total_bytes,
      p.prev_data_bytes,
      p.prev_index_bytes,
      p.prev_total_bytes,
      (
        SELECT COUNT(*)
        FROM (
          SELECT lt2.instance_id, t2.schema_name, SUM(t2.total_bytes) AS last_total_bytes
          FROM (
            SELECT instance_id, MAX(stat_time) AS last_time
            FROM ops_inspection.snap_mysql_table_topn
            GROUP BY instance_id
          ) lt2
          JOIN ops_inspection.snap_mysql_table_topn t2
            ON t2.instance_id = lt2.instance_id AND t2.stat_time = lt2.last_time
          WHERE t2.instance_id = l.instance_id
          GROUP BY lt2.instance_id, t2.schema_name
        ) tmp
        WHERE tmp.last_total_bytes > l.last_total_bytes
      ) AS higher_cnt
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
        WHERE collect_status = 'ok'
        GROUP BY instance_id
      ) ls ON CAST(ls.instance_id AS CHAR) = a.instance_id
      WHERE a.is_active = 1
    ) io
    JOIN (
      SELECT
        lt.instance_id,
        t.schema_name,
        SUM(t.data_bytes) AS last_data_bytes,
        SUM(t.index_bytes) AS last_index_bytes,
        SUM(t.total_bytes) AS last_total_bytes
      FROM (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_table_topn
        GROUP BY instance_id
      ) lt
      JOIN ops_inspection.snap_mysql_table_topn t
        ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
      GROUP BY lt.instance_id, t.schema_name
    ) l ON l.instance_id = io.instance_id
    LEFT JOIN (
      SELECT
        pt.instance_id,
        t.schema_name,
        SUM(t.data_bytes) AS prev_data_bytes,
        SUM(t.index_bytes) AS prev_index_bytes,
        SUM(t.total_bytes) AS prev_total_bytes
      FROM (
        SELECT s.instance_id, MAX(s.stat_time) AS prev_time
        FROM ops_inspection.snap_mysql_table_topn s
        JOIN (
          SELECT instance_id, MAX(stat_time) AS last_time
          FROM ops_inspection.snap_mysql_table_topn
          GROUP BY instance_id
        ) lt2 ON lt2.instance_id = s.instance_id AND s.stat_time < lt2.last_time
        GROUP BY s.instance_id
      ) pt
      JOIN ops_inspection.snap_mysql_table_topn t
        ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
      GROUP BY pt.instance_id, t.schema_name
    ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name
  ) b
  WHERE b.higher_cnt < 5
) res
ORDER BY (res.prev_total_gb IS NULL), res.prev_total_gb DESC, res.last_total_gb DESC;

-- =============================
-- Q5: 表维度当前容量 Top10（每实例先取 Top10，再全局排序）
-- =============================
SELECT
  res.env,
  res.alias_name,
  res.instance_name,
  res.schema_name,
  res.table_name,
  res.last_data_gb,
  res.last_index_gb,
  res.last_total_gb,
  res.prev_data_gb,
  res.prev_index_gb,
  res.prev_total_gb,
  res.diff_total_gb_fmt
FROM (
  SELECT
    base.env,
    base.alias_name,
    base.instance_name,
    base.instance_id,
    base.schema_name,
    base.table_name,
    ROUND(base.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
    ROUND(base.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
    ROUND(base.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(base.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
    ROUND(base.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
    ROUND(base.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    CASE
      WHEN base.prev_total_bytes IS NULL THEN '-'
      WHEN (base.last_total_bytes - base.prev_total_bytes) > 0 THEN CONCAT('+', ROUND((base.last_total_bytes - base.prev_total_bytes) / POW(1024, 3), 2))
      WHEN (base.last_total_bytes - base.prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(base.last_total_bytes - base.prev_total_bytes) / POW(1024, 3), 2))
      ELSE '0'
    END AS diff_total_gb_fmt
  FROM (
    SELECT
      io.env,
      io.alias_name,
      io.instance_name,
      io.instance_id,
      l.schema_name,
      l.table_name,
      l.last_data_bytes,
      l.last_index_bytes,
      l.last_total_bytes,
      p.prev_data_bytes,
      p.prev_index_bytes,
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
        WHERE collect_status = 'ok'
        GROUP BY instance_id
      ) ls ON CAST(ls.instance_id AS CHAR) = a.instance_id
      WHERE a.is_active = 1
    ) io
    JOIN (
      SELECT instance_id, MAX(stat_time) AS last_time
      FROM ops_inspection.snap_mysql_table_topn
      GROUP BY instance_id
    ) lt ON lt.instance_id = io.instance_id
    JOIN ops_inspection.snap_mysql_table_topn t
      ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
    GROUP BY io.instance_id, t.schema_name, t.table_name
  ) l
    JOIN io ON io.instance_id = l.instance_id
  LEFT JOIN (
    SELECT
      pt.instance_id,
      t.schema_name,
      t.table_name,
      SUM(t.data_bytes) AS prev_data_bytes,
      SUM(t.index_bytes) AS prev_index_bytes,
      SUM(t.total_bytes) AS prev_total_bytes
    FROM (
      SELECT s.instance_id, MAX(s.stat_time) AS prev_time
      FROM ops_inspection.snap_mysql_table_topn s
      JOIN (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_table_topn
        GROUP BY instance_id
      ) lt2 ON lt2.instance_id = s.instance_id AND s.stat_time < lt2.last_time
      GROUP BY s.instance_id
    ) pt
    JOIN ops_inspection.snap_mysql_table_topn t
      ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
    GROUP BY pt.instance_id, t.schema_name, t.table_name
  ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
  ) base
  WHERE (
    SELECT COUNT(*) FROM (
      SELECT lt2.instance_id, t2.schema_name, t2.table_name, SUM(t2.total_bytes) AS last_total_bytes
      FROM (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_table_topn
        GROUP BY instance_id
      ) lt2
      JOIN ops_inspection.snap_mysql_table_topn t2
        ON t2.instance_id = lt2.instance_id AND t2.stat_time = lt2.last_time
      WHERE t2.instance_id = base.instance_id
      GROUP BY lt2.instance_id, t2.schema_name, t2.table_name
    ) tmp WHERE tmp.last_total_bytes > base.last_total_bytes
  ) < 10
) res
ORDER BY (res.prev_total_gb IS NULL), res.prev_total_gb DESC, res.last_total_gb DESC;

-- =============================
-- Q6: 表维度近两次容量差异 Top10（每实例先取 Top10 by |diff|，再全局排序）
-- =============================
SELECT
  res.env,
  res.alias_name,
  res.instance_name,
  res.schema_name,
  res.table_name,
  res.last_data_gb,
  res.last_index_gb,
  res.last_total_gb,
  res.prev_data_gb,
  res.prev_index_gb,
  res.prev_total_gb,
  res.diff_total_gb_fmt
FROM (
  SELECT
    base.env,
    base.alias_name,
    base.instance_name,
    base.instance_id,
    base.schema_name,
    base.table_name,
    ROUND(base.last_data_bytes / POW(1024, 3), 2) AS last_data_gb,
    ROUND(base.last_index_bytes / POW(1024, 3), 2) AS last_index_gb,
    ROUND(base.last_total_bytes / POW(1024, 3), 2) AS last_total_gb,
    ROUND(base.prev_data_bytes / POW(1024, 3), 2) AS prev_data_gb,
    ROUND(base.prev_index_bytes / POW(1024, 3), 2) AS prev_index_gb,
    ROUND(base.prev_total_bytes / POW(1024, 3), 2) AS prev_total_gb,
    CASE
      WHEN base.prev_total_bytes IS NULL THEN '-'
      WHEN (base.last_total_bytes - base.prev_total_bytes) > 0 THEN CONCAT('+', ROUND((base.last_total_bytes - base.prev_total_bytes) / POW(1024, 3), 2))
      WHEN (base.last_total_bytes - base.prev_total_bytes) < 0 THEN CONCAT('-', ROUND(ABS(base.last_total_bytes - base.prev_total_bytes) / POW(1024, 3), 2))
      ELSE '0'
    END AS diff_total_gb_fmt,
    (base.last_total_bytes - IFNULL(base.prev_total_bytes, 0)) AS diff_bytes
  FROM (
    SELECT
      io.env,
      io.alias_name,
      io.instance_name,
      io.instance_id,
      l.schema_name,
      l.table_name,
      l.last_data_bytes,
      l.last_index_bytes,
      l.last_total_bytes,
      p.prev_data_bytes,
      p.prev_index_bytes,
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
        WHERE collect_status = 'ok'
        GROUP BY instance_id
      ) ls ON CAST(ls.instance_id AS CHAR) = a.instance_id
      WHERE a.is_active = 1
    ) io
    JOIN (
      SELECT
        lt.instance_id,
        t.schema_name,
        t.table_name,
        SUM(t.data_bytes) AS last_data_bytes,
        SUM(t.index_bytes) AS last_index_bytes,
        SUM(t.total_bytes) AS last_total_bytes
      FROM (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_table_topn
        GROUP BY instance_id
      ) lt
      JOIN ops_inspection.snap_mysql_table_topn t
        ON t.instance_id = lt.instance_id AND t.stat_time = lt.last_time
      GROUP BY lt.instance_id, t.schema_name, t.table_name
    ) l ON l.instance_id = io.instance_id
    LEFT JOIN (
      SELECT
        pt.instance_id,
        t.schema_name,
        t.table_name,
        SUM(t.data_bytes) AS prev_data_bytes,
        SUM(t.index_bytes) AS prev_index_bytes,
        SUM(t.total_bytes) AS prev_total_bytes
      FROM (
        SELECT s.instance_id, MAX(s.stat_time) AS prev_time
        FROM ops_inspection.snap_mysql_table_topn s
        JOIN (
          SELECT instance_id, MAX(stat_time) AS last_time
          FROM ops_inspection.snap_mysql_table_topn
          GROUP BY instance_id
        ) lt2 ON lt2.instance_id = s.instance_id AND s.stat_time < lt2.last_time
        GROUP BY s.instance_id
      ) pt
      JOIN ops_inspection.snap_mysql_table_topn t
        ON t.instance_id = pt.instance_id AND t.stat_time = pt.prev_time
      GROUP BY pt.instance_id, t.schema_name, t.table_name
    ) p ON p.instance_id = l.instance_id AND p.schema_name = l.schema_name AND p.table_name = l.table_name
  ) base
  WHERE (
    SELECT COUNT(*) FROM (
      SELECT lt2.instance_id, t2.schema_name, t2.table_name, (SUM(t2.total_bytes) - IFNULL(
        (
          SELECT SUM(t3.total_bytes)
          FROM (
            SELECT s.instance_id, MAX(s.stat_time) AS prev_time
            FROM ops_inspection.snap_mysql_table_topn s
            JOIN (
              SELECT instance_id, MAX(stat_time) AS last_time
              FROM ops_inspection.snap_mysql_table_topn
              GROUP BY instance_id
            ) l2 ON s.instance_id = l2.instance_id AND s.stat_time < l2.last_time
            GROUP BY s.instance_id
          ) pt3
          JOIN ops_inspection.snap_mysql_table_topn t3
            ON t3.instance_id = pt3.instance_id AND t3.stat_time = pt3.prev_time
          WHERE t3.instance_id = t2.instance_id
            AND t3.schema_name = t2.schema_name
            AND t3.table_name = t2.table_name
          GROUP BY t3.instance_id, t3.schema_name, t3.table_name
        ), 0)
      ) AS diff_bytes
      FROM (
        SELECT instance_id, MAX(stat_time) AS last_time
        FROM ops_inspection.snap_mysql_table_topn
        GROUP BY instance_id
      ) lt2
      JOIN ops_inspection.snap_mysql_table_topn t2
        ON t2.instance_id = lt2.instance_id AND t2.stat_time = lt2.last_time
      WHERE t2.instance_id = base.instance_id
      GROUP BY lt2.instance_id, t2.schema_name, t2.table_name
    ) tmp WHERE ABS(tmp.diff_bytes) > ABS(base.last_total_bytes - IFNULL(base.prev_total_bytes, 0))
  ) < 10
) res
ORDER BY ABS(res.last_total_gb - IFNULL(res.prev_total_gb, 0)) DESC, res.env, res.alias_name, res.instance_name, res.schema_name, res.table_name;
