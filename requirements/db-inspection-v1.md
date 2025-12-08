1
# DB 巡检 V1 需求说明（供 Codex 持续迭代）

> 目标：用 **Shell + mysql client** 做低频（天/周级）巡检，聚焦 **容量总览 + 大表 TOPN + 增长趋势**。
> 本版本不做“监控重复项”（CPU/QPS/实时锁等待等），也不做深度结构治理。

---

## 1. 总体范围

### 1.1 本期覆盖的软件类型

* **MySQL（必须）**
* **Redis / MongoDB（资产结构预留，采集逻辑后续迭代）**

### 1.2 本期巡检数据产出

* **实例级容量总览快照**：用于趋势/增量分析
* **大表明细快照**：每次采集 **Top 20**（可扩展“阈值表入库”）
* **实例资源清单信息**（最小）：版本

### 1.3 明确不做（V1）

* 实时性能类指标（CPU/QPS/IOPS/线程即时阈值）
* 慢查询明细（由外部 API 体系提供）
* 复杂结构风险规则（无主键、冗余索引等）
* 深度锁等待诊断

---

## 2. 数据模型设计

> 统一使用一个“巡检元数据库/Schema”，建议命名：

* **`ops_inspection`**（专业、清晰、可扩展）

所有表名采用：

* `asset_` 前缀：资产/元数据
* `snap_` 前缀：巡检快照（事实表）

### 2.1 资产表（统一多数据库）

**表名**：`ops_inspection.asset_instance`

**用途**：

* 统一管理 MySQL/Redis/Mongo 等实例资产
* 提供巡检筛选与展示维度（instance_name/alias_name/env）
* 为巡检脚本提供连接坐标与认证模式选择
* **不在元数据库中保存明文密码**（密码仅允许存在于本地安全配置文件用于初始化）

**字段定义建议（按你最新口径收敛）**

* `instance_id` INT PK AUTO_INCREMENT
* `type` ENUM('mysql','redis','mongodb') NOT NULL DEFAULT 'mysql'
* `instance_name` VARCHAR(25) NOT NULL
* `alias_name` VARCHAR(25) NULL
* `env` ENUM('MOS','Purple','RTM','MIB2') NOT NULL
* `host` VARCHAR(50) NOT NULL
* `port` INT NOT NULL

**认证与凭据管理（通用抽象）**

* `auth_mode` ENUM('login_path','local_secret','secret_ref','password') NOT NULL DEFAULT 'login_path'

  * 说明：

    * `login_path`：用于 MySQL
    * `local_secret`：Redis/Mongo V1 可用（巡检机本地安全文件）
    * `secret_ref`：未来对接云端 Secrets/KMS
    * `password`：不推荐，仅兼容兜底
* `username` VARCHAR(25) NULL

  * 说明：仅作展示/对账；当 `auth_mode=login_path` 时可为空
* `login_path` VARCHAR(25) NULL

  * 说明：仅 MySQL 有意义；**可由初始化脚本自动生成/回填**
* `secret_ref` VARCHAR(50) NULL

  * 说明：未来对接外部密钥系统时使用

**巡检控制**

* `is_active` TINYINT NOT NULL DEFAULT 1

**审计/维护字段**

* `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
* `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP

**初始化配置文件（无需手填的字段）**

* 以下字段应由初始化脚本自动填充或使用默认值：

  * `instance_id`（自增）
  * `type`（默认 mysql）
  * `auth_mode`（默认 login_path）
  * `is_active`（默认 1）

---

### 2.2 MySQL 实例容量快照表

**表名**：`ops_inspection.snap_mysql_instance_storage`

**粒度**：每实例**每次执行**一条（支持一天多次/一周一次）

**时间字段选择**

* 采用 `stat_time` **DATETIME**：

  * 原因：

    * 低频巡检可能不严格按“每天固定一次”执行
    * DATETIME 方便保留执行时刻、支持灵活的日/周聚合
    * 后续可 `DATE(stat_time)` 衍生日粒度

**字段建议（长度收敛）**

* `id` INT PK AUTO_INCREMENT
* `stat_time` DATETIME NOT NULL
* `instance_id` VARCHAR(25) NOT NULL
* `logical_data_bytes` BIGINT NOT NULL
* `logical_index_bytes` BIGINT NOT NULL
* `logical_total_bytes` BIGINT NOT NULL
* `mysql_version` VARCHAR(32) NULL

  * 说明：作为“资源清单观测值”，**不写回资产表**
* `collect_status` ENUM('ok','failed') NOT NULL DEFAULT 'ok'
* `error_msg` VARCHAR(255) NULL

**索引/约束建议**

* 索引：`idx_instance_time (instance_id, stat_time)`

---

### 2.3 MySQL 大表 TopN 快照表

**表名**：`ops_inspection.snap_mysql_table_topn`

**粒度**：每实例**每次执行**的 TopN 表明细

**字段建议（长度收敛）**

* `id` INT PK AUTO_INCREMENT
* `stat_time` DATETIME NOT NULL
* `instance_id` VARCHAR(25) NOT NULL
* `schema_name` VARCHAR(64) NOT NULL
* `table_name` VARCHAR(64) NOT NULL
* `engine` VARCHAR(16) NULL
* `table_rows` BIGINT NULL
* `data_bytes` BIGINT NOT NULL
* `index_bytes` BIGINT NOT NULL
* `total_bytes` BIGINT NOT NULL
* `rank_no` INT NOT NULL

**索引/约束建议**

* 索引：`idx_instance_time_rank (instance_id, stat_time, rank_no)`

**V1 采集规则**

* 默认只入库 **Top 20**
* 可选增强（V1.1）：同时入库 `total_bytes >= X GB` 的表（阈值可配置）

---

## 3. 认证与安全策略

MySQL 实例容量快照表

**表名**：`ops_inspection.snap_mysql_instance_storage_daily`

**粒度**：每实例每日（或每次执行日期）一条

**字段建议**

* `stat_date` DATE NOT NULL
* `instance_id` VARCHAR(128) NOT NULL
* `logical_data_bytes` BIGINT NOT NULL
* `logical_index_bytes` BIGINT NOT NULL
* `logical_total_bytes` BIGINT NOT NULL
* `mysql_version` VARCHAR(64) NULL

  * 说明：作为“资源清单观测值”，**不写回资产表**
* `collect_status` VARCHAR(32) NOT NULL DEFAULT 'ok'
* `error_msg` VARCHAR(1024) NULL

**唯一约束**

* `(stat_date, instance_id)`

### 2.3 MySQL 大表 TopN 快照表

**表名**：`ops_inspection.snap_mysql_table_topn_daily`

**粒度**：每实例每日 TopN 表明细

**字段建议**

* `stat_date` DATE NOT NULL
* `instance_id` VARCHAR(128) NOT NULL
* `schema_name` VARCHAR(128) NOT NULL
* `table_name` VARCHAR(128) NOT NULL
* `engine` VARCHAR(32) NULL
* `table_rows` BIGINT NULL
* `data_bytes` BIGINT NOT NULL
* `index_bytes` BIGINT NOT NULL
* `total_bytes` BIGINT NOT NULL
* `rank_no` INT NOT NULL

**唯一约束**

* `(stat_date, instance_id, rank_no)`

**V1 采集规则**

* 默认只入库 **Top 20**
* 可选增强（V1.1）：同时入库 `total_bytes >= X GB` 的表（阈值可配置）

---

## 3. 认证与安全策略

### 3.1 MySQL 使用 `mysql_config_editor` 的正确理解

**关键事实**：

* `mysql_config_editor` **不会为每个实例生成一个文件**。
* 所有 `login-path` 条目 **集中存储在同一个**：

  * `~/.mylogin.cnf`
* 该文件为 MySQL 客户端支持的加密/保护格式，避免明文出现在脚本或配置中。

### 3.2 资产表与 login-path 的对齐规则

* 资产表保存：

  * `instance_id / instance_name / alias_name / host / port / login_path / is_active`
* Shell 连接时：

  * `mysql --login-path=${login_path}`
* **落库标识**：

  * 统一使用 `instance_id` 作为事实表外键
  * `instance_name/alias_name` 只在分析 SQL 或视图中通过 JOIN 展示

### 3.3 新实例登录信息的初始化方式（两种模式）

#### 模式 A（推荐 V1，最省开发/最安全）

**人工一次性初始化 login-path**

* 原因：

  * 你目前不希望在元数据库存明文密码
  * 自动化前提是“脚本能拿到安全来源的密码”
* 流程：

  1. 资产表新增实例（含 login_path）
  2. 运维机上执行一次 `mysql_config_editor set ...`
  3. 巡检脚本即可自动识别并采集

#### 模式 B（V1.1 可选增强）

**自动感知资产表新增实例并初始化 login-path**

**前置安全条件必须满足其一**：

* B1：对接外部 Secret（`auth_mode=secret_ref`）
* B2：使用本地安全凭据文件（`auth_mode=local_secret`），文件权限 600，按 `instance_id` 索引

**逻辑要求**：

* 巡检脚本启动时：

  1. 查询 `asset_instance` 找出 `type=mysql AND is_active=1 AND auth_mode IN (...)`
  2. 检测本机 `mysql_config_editor` 是否已存在对应 `login_path`
  3. 对缺失的条目：

     * 从安全来源读取用户名/密码
     * 自动执行 `mysql_config_editor set`

**注意**：

* **禁止**从元数据库读取明文密码
* 自动化失败要记录日志，并将实例标记为采集失败（写 error_msg 到快照表）

### 3.4 Redis/Mongo 的 V1 凭据建议

* 资产表保留 `auth_mode` 抽象即可
* V1 不强制实现采集
* 未来采集时推荐：

  * `auth_mode=local_secret`：

    * 巡检机本地 `secrets.json` 或 `secrets.ini`（严格权限）
    * 资产表仅保存引用 key
  * 或 `auth_mode=secret_ref` 对接云端 Secrets 系统

---

## 4. 巡检脚本功能需求（MySQL V1）

### 4.1 脚本输入

* 运行参数（可选）：

  * `--type mysql`
  * `--stat-date YYYY-MM-DD`（默认今天）
* 配置文件（本地）：

  * 元数据库连接信息
  * TopN 数量（默认 20）
  * 大表阈值（可选）
  * 连接超时/重试次数

### 4.2 核心流程

1. 连接元数据库（ops_inspection）
2. **检查并创建库/表**（如果不存在）

   * `ops_inspection` schema
   * `asset_instance`
   * `snap_mysql_instance_storage_daily`
   * `snap_mysql_table_topn_daily`
3. 查询资产表：

   * `SELECT * FROM asset_instance WHERE type='mysql' AND is_active=1`
4. 对每个实例：

   * 根据 `auth_mode` 选择连接策略：

     * `login_path` → `mysql --login-path=xxx`
     * 其他模式暂记录为 not_supported 或预留实现
   * 采集两类数据：

     * A) 实例逻辑容量汇总（information_schema 汇总）
     * B) 表级容量排序取 TopN
   * 写入快照表
5. 输出执行摘要日志（成功/失败数量）

### 4.3 错误与降级

* 单实例连接失败不能中断全局执行
* 失败实例仍写一条实例快照：

  * `collect_status='failed'` + `error_msg`

---

## 5. 分析 SQL 需求（供后续报表/看板）

### 5.1 实例容量总览

* 当前最新容量（按 instance_id JOIN asset_instance 展示 name/alias/cluster/env）

### 5.2 近 7/14 天增长

* 基于 `snap_mysql_instance_storage_daily` 做自关联或窗口计算

### 5.3 大表增长排行

* 基于 `snap_mysql_table_topn_daily` 对比不同 stat_date 的 total_bytes

---

## 6. 验收标准（V1）

1. 资产表满足：

   * 字段：`type/instance_id/instance_name/alias_name/host/port/is_active/auth_mode/login_path` 等
   * 可同时存放 MySQL/Redis/Mongo 资产

2. 巡检脚本满足：

   * 能自动创建 `ops_inspection` 与三张表
   * 能读取 `asset_instance` 筛选 MySQL 实例
   * 能输出并落库：

     * 实例级逻辑容量快照
     * Top 20 大表快照
   * 失败实例有落库记录（状态+错误）

3. 分析侧：

   * 提供至少 3 条示例分析 SQL：

     * 最新实例容量总览
     * 近 7 天日均增长
     * 大表增长 TopN

---

## 7. 任务拆解（给 Codex 的执行清单）

* [x] 设计并输出 DDL：`ops_inspection` + 3 张表
* [x] 输出采集 SQL（实例汇总 + TopN 大表）
* [x] 设计配置文件样例
* [x] 编写 Shell 主流程（含重试/超时/日志）
* [x] 实现“库/表不存在则创建”逻辑
* [x] 实现 `login_path` 连接模式
* [x] 输出 3 条示例分析 SQL
* [ ] 本地自测说明（如何验证数据正确性）

---

## 8. 版本演进建议

### V1.1（可选）

* 支持“大表阈值表入库”
* 支持 `auth_mode=local_secret/secret_ref` 的自动 login-path 初始化

### V2（多数据库采集）

* 新增：

  * `snap_redis_instance_storage_daily`
  * `snap_mongodb_instance_storage_daily`
* 复用同一资产表与巡检框架

---

## 9. 给你的使用提示（非功能要求）

* 你可以把本文件保存为：

  * `requirements/db-inspection-v1.md`
* 与 Codex 协作时，让它：

  * 每完成一项任务就在“任务拆解清单”中打勾
  * 在本地 Git 提交中体现进度（如果你有 repo）

> 说明：我无法直接替你“实时编辑本地文件”，但你可以用本需求文档作为 **唯一事实来源（SSOT）**，让 Codex 按清单推进并自行更新该文件。
