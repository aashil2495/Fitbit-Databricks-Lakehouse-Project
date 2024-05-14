-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL Notebook for Bronze Layer Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the tables

-- COMMAND ----------

-- Create customer table
CREATE TABLE if not exists bronze_demo.default.customer (
  c_custkey bigint,
  c_name string,
  c_address string,
  c_nationkey bigint,
  c_phone string,
  c_acctbal decimal(18,2),
  c_mktsegment string,
  c_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);


-- Create lineitem table
CREATE TABLE if not exists bronze_demo.default.lineitem (
  l_orderkey bigint,
  l_partkey bigint,
  l_suppkey bigint,
  l_linenumber int,
  l_quantity decimal(18,2),
  l_extendedprice decimal(18,2),
  l_discount decimal(18,2),
  l_tax decimal(18,2),
  l_returnflag string,
  l_linestatus string,
  l_shipdate date,
  l_commitdate date,
  l_receiptdate date,
  l_shipinstruct string,
  l_shipmode string,
  l_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create nation table
CREATE TABLE if not exists bronze_demo.default.nation (
  n_nationkey bigint,
  n_name string,
  n_regionkey bigint,
  n_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create orders table
CREATE TABLE if not exists bronze_demo.default.orders (
  o_orderkey bigint,
  o_custkey bigint,
  o_orderstatus string,
  o_totalprice decimal(18,2),
  o_orderdate date,
  o_orderpriority string,
  o_clerk string,
  o_shippriority int,
  o_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create part table
CREATE TABLE if not exists bronze_demo.default.part (
  p_partkey bigint,
  p_name string,
  p_mfgr string,
  p_brand string,
  p_type string,
  p_size int,
  p_container string,
  p_retailprice decimal(18,2),
  p_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create partsupp table
CREATE TABLE if not exists bronze_demo.default.partsupp (
  ps_partkey bigint,
  ps_suppkey bigint,
  ps_availqty int,
  ps_supplycost decimal(18,2),
  ps_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create region table
CREATE TABLE if not exists bronze_demo.default.region (
  r_regionkey bigint,
  r_name string,
  r_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- Create supplier table
CREATE TABLE if not exists bronze_demo.default.supplier (
  s_suppkey bigint,
  s_name string,
  s_address string,
  s_nationkey bigint,
  s_phone string,
  s_acctbal decimal(18,2),
  s_comment string,
  last_modified_dt_time timestamp,
  last_added_dt_time TIMESTAMP
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Loading data in tables from tpch database

-- COMMAND ----------

insert into bronze_demo.default.customer select *,current_timestamp(),current_timestamp() from samples.tpch.customer;
insert into bronze_demo.default.lineitem  select *,current_timestamp(),current_timestamp() from samples.tpch.lineitem;
insert into bronze_demo.default.nation  select *,current_timestamp(),current_timestamp() from samples.tpch.nation;
insert into bronze_demo.default.orders  select *,current_timestamp(),current_timestamp() from samples.tpch.orders;
insert into bronze_demo.default.part  select *,current_timestamp(),current_timestamp() from samples.tpch.part;
insert into bronze_demo.default.partsupp  select *,current_timestamp(),current_timestamp() from samples.tpch.partsupp;
insert into bronze_demo.default.region  select *,current_timestamp(),current_timestamp() from samples.tpch.region;
insert into bronze_demo.default.supplier  select *,current_timestamp(),current_timestamp() from samples.tpch.supplier;

-- COMMAND ----------


