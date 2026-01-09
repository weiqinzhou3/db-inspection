-- Schema and table DDL for ops_inspection
-- schema & table logical mapping
-- @schema OPS_INSPECTION_DB=ops_inspection
-- @table ASSET_INSTANCE=asset_instance
-- @table ASSET_MONGO_CONN=asset_mongo_conn
-- @table SNAP_MYSQL_INSTANCE_STORAGE=snap_mysql_instance_storage
-- @table SNAP_MYSQL_TABLE_TOPN=snap_mysql_table_topn
-- @table SNAP_MONGO_INSTANCE_STORAGE=snap_mongo_instance_storage
-- @table SNAP_MONGO_COLLECTION_TOPN=snap_mongo_collection_topn

-- Create schema
CREATE DATABASE IF NOT EXISTS ops_inspection
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE ops_inspection;

-- Asset table: supports MySQL/Redis/Mongo assets without storing passwords
CREATE TABLE IF NOT EXISTS asset_instance (
  instance_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  type ENUM('mysql', 'redis', 'mongodb') NOT NULL DEFAULT 'mysql',
  instance_name VARCHAR(50) NOT NULL,
  alias_name VARCHAR(50) NULL,
  env ENUM('MOS', 'Purple', 'RTM', 'MIB2') DEFAULT NULL,
  host VARCHAR(50) NOT NULL,
  port INT NOT NULL,
  auth_mode ENUM('login_path', 'local_secret', 'secret_ref', 'password', 'mongo_uri_aes') NOT NULL DEFAULT 'login_path',
  username VARCHAR(50) NULL,
  login_path VARCHAR(50) NULL,
  secret_ref VARCHAR(50) NULL,
  is_active TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Asset table: add mongo URI auth mode if needed (safe to run repeatedly)
ALTER TABLE asset_instance
  MODIFY auth_mode ENUM('login_path', 'local_secret', 'secret_ref', 'password', 'mongo_uri_aes') NOT NULL DEFAULT 'login_path';

-- MySQL instance storage snapshot table
CREATE TABLE IF NOT EXISTS snap_mysql_instance_storage (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stat_time DATETIME NOT NULL,
  instance_id INT NOT NULL,
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
  instance_id INT NOT NULL,
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

-- Mongo asset connection table (encrypted URI only)
CREATE TABLE IF NOT EXISTS asset_mongo_conn (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  instance_id BIGINT NOT NULL,
  conn_name VARCHAR(128) NOT NULL,
  mongo_uri_enc TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_instance (instance_id)
);

-- Mongo instance storage snapshot table
CREATE TABLE IF NOT EXISTS snap_mongo_instance_storage (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stat_time DATETIME NOT NULL,
  instance_id BIGINT NOT NULL,
  logical_data_bytes BIGINT NOT NULL,
  logical_index_bytes BIGINT NOT NULL,
  logical_total_bytes BIGINT NOT NULL,
  physical_total_bytes BIGINT NOT NULL,
  mongo_version VARCHAR(32) NOT NULL,
  collect_status VARCHAR(16) NOT NULL,
  error_msg VARCHAR(512) NULL,
  KEY idx_instance_time (instance_id, stat_time)
);

-- Mongo collection TopN snapshot table
CREATE TABLE IF NOT EXISTS snap_mongo_collection_topn (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stat_time DATETIME NOT NULL,
  instance_id BIGINT NOT NULL,
  db_name VARCHAR(128) NOT NULL,
  coll_name VARCHAR(256) NOT NULL,
  doc_count BIGINT NOT NULL,
  data_bytes BIGINT NOT NULL,
  index_bytes BIGINT NOT NULL,
  logical_total_bytes BIGINT NOT NULL,
  physical_total_bytes BIGINT NOT NULL,
  KEY idx_instance_time (instance_id, stat_time),
  KEY idx_db_coll (instance_id, db_name, coll_name)
);
