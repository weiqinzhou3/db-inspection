-- Schema and table DDL for ops_inspection
-- schema & table logical mapping
-- @schema OPS_INSPECTION_DB=ops_inspection
-- @table ASSET_INSTANCE=asset_instance
-- @table SNAP_MYSQL_INSTANCE_STORAGE=snap_mysql_instance_storage
-- @table SNAP_MYSQL_TABLE_TOPN=snap_mysql_table_topn

-- Create schema
CREATE DATABASE IF NOT EXISTS ops_inspection
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ops_inspection;

-- Asset table: supports MySQL/Redis/Mongo assets without storing passwords
CREATE TABLE IF NOT EXISTS asset_instance (
  instance_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  type ENUM('mysql', 'redis', 'mongodb') NOT NULL DEFAULT 'mysql',
  instance_name VARCHAR(25) NOT NULL,
  alias_name VARCHAR(25) NULL,
  env ENUM('MOS', 'Purple', 'RTM', 'MIB2') DEFAULT NULL,
  host VARCHAR(50) NOT NULL,
  port INT NOT NULL,
  auth_mode ENUM('login_path', 'local_secret', 'secret_ref', 'password') NOT NULL DEFAULT 'login_path',
  username VARCHAR(25) NULL,
  login_path VARCHAR(25) NULL,
  secret_ref VARCHAR(50) NULL,
  is_active TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- MySQL instance storage snapshot table
CREATE TABLE IF NOT EXISTS snap_mysql_instance_storage (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stat_time DATETIME NOT NULL,
  instance_id VARCHAR(25) NOT NULL,
  logical_data_bytes BIGINT NOT NULL,
  logical_index_bytes BIGINT NOT NULL,
  logical_total_bytes BIGINT NOT NULL,
  mysql_version VARCHAR(32) NULL,
  collect_status ENUM('ok', 'failed') NOT NULL DEFAULT 'ok',
  error_msg VARCHAR(255) NULL,
  KEY idx_instance_time (instance_id, stat_time)
);

-- MySQL table TopN snapshot table
CREATE TABLE IF NOT EXISTS snap_mysql_table_topn (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stat_time DATETIME NOT NULL,
  instance_id VARCHAR(25) NOT NULL,
  schema_name VARCHAR(64) NOT NULL,
  table_name VARCHAR(64) NOT NULL,
  engine VARCHAR(16) NULL,
  table_rows BIGINT NULL,
  data_bytes BIGINT NOT NULL,
  index_bytes BIGINT NOT NULL,
  total_bytes BIGINT NOT NULL,
  rank_no INT NOT NULL,
  KEY idx_instance_time_rank (instance_id, stat_time, rank_no)
);
