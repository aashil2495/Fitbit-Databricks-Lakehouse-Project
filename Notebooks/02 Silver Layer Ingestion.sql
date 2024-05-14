-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL Notebook for Silver Layer Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the tables

-- COMMAND ----------

create table if not exists silver_demo.default.customer(
  customerkey int,
  name string,
  address string,
  nationkey int,
  phone string,
  acctbal double,
  mktsegment string,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);

CREATE TABLE IF NOT EXISTS silver_demo.default.lineitem(
  orderkey INT not NULL,
  partkey INT not null,
  suppkey INT not null,
  linenumber INT,
  quantity INT,
  extendedprice DOUBLE,
  discount DOUBLE,
  tax DOUBLE,
  returnflag STRING,
  linestatus STRING,
  shipdate DATE,
  commitdate DATE,
  receiptdate DATE,
  shipinstruct STRING,
  shipmode STRING,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.nation (
  nationkey INT,
  name STRING,
  region INT,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.orders (
  orderkey INT,
  custkey INT,
  orderstatus STRING,
  totalprice DOUBLE,
  orderdate DATE,
  orderpriority STRING,
  shippriority INT,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.part (
  partkey INT,
  name STRING,
  mfgr STRING,
  brand STRING,
  type STRING,
  size INT,
  container STRING,
  retailprice DOUBLE,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.partsupp (
  partkey INT not null,
  suppkey INT not NULL,
  availqty INT,
  supplycost DOUBLE,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.region (
  regionkey INT,
  name STRING,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);
CREATE TABLE IF NOT EXISTS silver_demo.default.supplier (
  suppkey INT,
  name STRING,
  address STRING,
  nationkey INT,
  phone STRING,
  acctbal DOUBLE,
  last_modified_dt_time timestamp,
  last_added_dt_time timestamp
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Loading data into tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.customer').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC   spark.sql('insert into silver_demo.default.customer select c_custkey, \
-- MAGIC   c_name, \
-- MAGIC   c_address , \
-- MAGIC   c_nationkey , \
-- MAGIC   c_phone , \
-- MAGIC   c_acctbal , \
-- MAGIC   c_mktsegment , \
-- MAGIC   current_timestamp(),current_timestamp() from broze_demo.default.customer')
-- MAGIC else:
-- MAGIC   spark.sql('insert into silver_demo.default.customer select  c_custkey, \
-- MAGIC   c_name, \
-- MAGIC   c_address , \
-- MAGIC   c_nationkey , \
-- MAGIC   c_phone , \
-- MAGIC   c_acctbal , \
-- MAGIC   c_mktsegment , \
-- MAGIC   current_timestamp(),current_timestamp() from broze_demo.default.customer \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.customer)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.lineitem').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.lineitem select l_orderkey ,\
-- MAGIC   l_partkey ,\
-- MAGIC   l_suppkey ,\
-- MAGIC   l_linenumber ,\
-- MAGIC   l_quantity ,\
-- MAGIC   l_extendedprice ,\
-- MAGIC   l_discount ,\
-- MAGIC   l_tax ,\
-- MAGIC   l_returnflag ,\
-- MAGIC   l_linestatus ,\
-- MAGIC   l_shipdate ,\
-- MAGIC   l_commitdate ,\
-- MAGIC   l_receiptdate ,\
-- MAGIC   l_shipinstruct ,\
-- MAGIC   l_shipmode,\
-- MAGIC   current_timestamp(),current_timestamp() from bronze_demo.default.lineitem')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.lineitem select  l_orderkey ,\
-- MAGIC   l_partkey ,\
-- MAGIC   l_suppkey ,\
-- MAGIC   l_linenumber ,\
-- MAGIC   l_quantity ,\
-- MAGIC   l_extendedprice ,\
-- MAGIC   l_discount ,\
-- MAGIC   l_tax ,\
-- MAGIC   l_returnflag ,\
-- MAGIC   l_linestatus ,\
-- MAGIC   l_shipdate ,\
-- MAGIC   l_commitdate ,\
-- MAGIC   l_receiptdate ,\
-- MAGIC   l_shipinstruct ,\
-- MAGIC   l_shipmode,\
-- MAGIC   current_timestamp(),current_timestamp() from bronze_demo.default.lineitem \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.lineitem)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.nation').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.nation select n_nationkey ,\
-- MAGIC     n_name ,\
-- MAGIC     n_regionkey,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.nation')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.nation select  n_nationkey ,\
-- MAGIC     n_name ,\
-- MAGIC     n_regionkey,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.nation \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.nation)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.orders').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.orders select o_orderkey ,\
-- MAGIC     o_custkey ,\
-- MAGIC     o_orderstatus ,\
-- MAGIC     o_totalprice ,\
-- MAGIC     o_orderdate ,\
-- MAGIC     o_orderpriority ,\
-- MAGIC     o_shippriority ,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.orders')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.orders select o_orderkey ,\
-- MAGIC     o_custkey ,\
-- MAGIC     o_orderstatus ,\
-- MAGIC     o_totalprice ,\
-- MAGIC     o_orderdate ,\
-- MAGIC     o_orderpriority ,\
-- MAGIC     o_shippriority ,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.orders \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.orders)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.part').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.part select p_partkey ,\
-- MAGIC     p_name ,\
-- MAGIC     p_mfgr ,\
-- MAGIC     p_brand ,\
-- MAGIC     p_type ,\
-- MAGIC     p_size ,\
-- MAGIC     p_container ,\
-- MAGIC     p_retailprice ,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.part')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.part select p_partkey ,\
-- MAGIC     p_name ,\
-- MAGIC     p_mfgr ,\
-- MAGIC     p_brand ,\
-- MAGIC     p_type ,\
-- MAGIC     p_size ,\
-- MAGIC     p_container ,\
-- MAGIC     p_retailprice ,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.part \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.part)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.partsupp').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.partsupp select \
-- MAGIC     ps_partkey ,\
-- MAGIC     ps_suppkey ,\
-- MAGIC     ps_availqty ,\
-- MAGIC     ps_supplycost ,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.partsupp')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.partsupp select \
-- MAGIC     ps_partkey ,\
-- MAGIC     ps_suppkey ,\
-- MAGIC     ps_availqty ,\
-- MAGIC     ps_supplycost ,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.partsupp \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.partsupp)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.region').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.region select \
-- MAGIC     r_regionkey ,\
-- MAGIC     r_name ,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.region')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.region select \
-- MAGIC     r_regionkey ,\
-- MAGIC     r_name ,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.region \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.region)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from silver_demo.default.supplier').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into silver_demo.default.supplier select \
-- MAGIC     s_suppkey ,\
-- MAGIC     s_name ,\
-- MAGIC     s_address ,\
-- MAGIC     s_nationkey ,\
-- MAGIC     s_phone ,\
-- MAGIC     s_acctbal ,\
-- MAGIC     current_timestamp(),current_timestamp() \
-- MAGIC     from bronze_demo.default.supplier')
-- MAGIC else:
-- MAGIC     spark.sql('insert into silver_demo.default.supplier select \
-- MAGIC     s_suppkey ,\
-- MAGIC     s_name ,\
-- MAGIC     s_address ,\
-- MAGIC     s_nationkey ,\
-- MAGIC     s_phone ,\
-- MAGIC     s_acctbal ,\
-- MAGIC     current_timestamp(),current_timestamp() from bronze_demo.default.supplier \
-- MAGIC             where last_added_dt_time> (select max(last_added_dt_time) from silver_demo.default.supplier)')
-- MAGIC
