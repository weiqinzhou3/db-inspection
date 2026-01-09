# MongoDB Inspection Guide (Step-by-Step)

本文档描述 MongoDB 巡检链路的端到端流程：资产注册 → 远程采集 → 数据落表 → SQL 分析 → TSV 导出。

## 1) 整体架构说明

流程概览：

1. 准备 `config/mongo/mongo-init.yaml`（包含明文 URI）
2. `scripts/mongo/init_mongo_assets.sh` 写入 `asset_instance`
3. `scripts/mongo/run_mongo_inspection.sh` 解密 URI 并采集容量信息
4. `sql/mongo/analysis/mongo_analysis.sql` 负责分析查询
5. `scripts/mongo/export_mongo_analysis_tsv.sh` 导出 TSV

## 2) 配置与安全

### 2.1 统一使用完整 URI 连接

所有 Mongo 连接必须使用完整 URI，支持副本集和分片集群，例如：

- `mongodb://user:pwd@host1:3717,host2:3717/admin?replicaSet=xxx`
- `mongodb://user:pwd@s-xxx1:3717,s-xxx2:3717,s-xxx3:3717/admin`

脚本不会拆分/重组 host/port，也不会拼接 URI。

### 2.2 明文 URI 的加密方式

`ops_inspection.asset_instance.login_path` 仅保存 `mongo_uri_enc`（Base64 + AES-256-CBC）：

```
printf "%s" "$MONGO_URI" | openssl enc -aes-256-cbc -base64 \
  -K "$MONGO_AES_KEY_HEX" \
  -iv "$MONGO_AES_IV_HEX"
```

解密示例：

```
printf "%s" "$mongo_uri_enc" | openssl enc -d -aes-256-cbc -base64 \
  -K "$MONGO_AES_KEY_HEX" \
  -iv "$MONGO_AES_IV_HEX"
```

### 2.3 KEY/IV 配置说明

`MONGO_AES_KEY_HEX` / `MONGO_AES_IV_HEX` 必须通过环境变量提供，不要写死在仓库中。
运行巡检脚本时注入即可。

## 3) Mongo 巡检字段说明

### 3.1 asset_instance（Mongo 部分）

- `type='mongo'`
- `auth_mode='mongo_uri_aes'`
- `login_path`: AES-256-CBC + Base64 加密后的 Mongo URI

### 3.2 snap_mongo_instance_storage

- `logical_data_bytes`: 非系统库数据大小之和
- `logical_index_bytes`: 非系统库索引大小之和
- `logical_total_bytes`: data + index
- `physical_total_bytes`: 物理占用（按 collection 的 storageSize + indexSize 估算）
- `mongo_version`: serverStatus().version
- `collect_status`: ok / failed
- `error_msg`: 采集失败原因

### 3.3 snap_mongo_collection_topn

- `db_name` / `coll_name`: 库/集合
- `doc_count`: 文档数量
- `data_bytes` / `index_bytes`: 逻辑大小
- `logical_total_bytes`: data + index
- `physical_total_bytes`: storageSize + indexSize

### 3.4 分析 SQL 字段说明

`sql/mongo/analysis/mongo_analysis.sql` 中的 `diff_*_gb_fmt` 字段规则：

- > 0: `+X.Y`
- < 0: `-X.Y`
- = 0: `0`
- 无 prev: `-`

所有容量字段均按 `ROUND(bytes / POW(1024, 3), 2)` 转为 GB。

## 4) 操作步骤（傻瓜式）

### Step 1: 准备配置

```
cp config/mongo/mongo-init.yaml.example config/mongo/mongo-init.yaml
```

填写 `mongo_uri` 为明文 URI，脚本会在写入数据库时自动加密。

### Step 2: 录入 Mongo 资产

```
MONGO_AES_KEY_HEX=... MONGO_AES_IV_HEX=... \
OPS_META_LOGIN_PATH=ops_meta ./scripts/mongo/init_mongo_assets.sh
```

该脚本只写入加密 URI，不会解密。

### Step 3: 执行 Mongo 巡检

```
MONGO_AES_KEY_HEX=... MONGO_AES_IV_HEX=... \
OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection \
./scripts/mongo/run_mongo_inspection.sh
```

### Step 4: 导出分析 TSV

```
OPS_META_LOGIN_PATH=ops_meta OPS_META_DB=ops_inspection \
./scripts/mongo/export_mongo_analysis_tsv.sh
```

输出目录：`out/mongo/analysis/`，包含 4 个 TSV 文件。

### Step 5: 直接执行分析 SQL

```
source config/schema_env.sh
envsubst < sql/mongo/analysis/mongo_analysis.sql | mysql --login-path=ops_meta -D "$OPS_INSPECTION_DB"
```
