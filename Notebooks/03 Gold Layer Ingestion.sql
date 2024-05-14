-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SQL Notebook for Gold Layer Ingestion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating the tables

-- COMMAND ----------

CREATE TABLE if not exists gold_demo.default.Dim_Part (
    partkey INT,
    name STRING,
    mfgr STRING,
    brand STRING,
    type STRING,
    size INT,
    container STRING,
    retailprice FLOAT,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not EXISTS gold_demo.default.Dim_Nation (
    nationkey INT,
    nation STRING,
    region STRING,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not exists gold_demo.default.Dim_Shipmode (
    shipmodekey BIGINT GENERATED ALWAYS AS IDENTITY,
    shipmodevalue STRING,
    last_added_dt_time TIMESTAMP
);


CREATE TABLE if not exists gold_demo.default.Dim_ShipInstruct (
    shipinstructkey BIGINT GENERATED ALWAYS AS IDENTITY,
    shipinstructvalue STRING,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not exists gold_demo.default.Dim_ReturnFlag (
    returnflagkey BIGINT GENERATED ALWAYS AS IDENTITY,
    returnflagvalue STRING,
    last_added_dt_time TIMESTAMP
);

---
CREATE TABLE if not exists gold_demo.default.Dim_OrderStatus (
    orderstatuskey BIGINT GENERATED ALWAYS AS IDENTITY,
    orderstatusvalue STRING,
    last_added_dt_time TIMESTAMP
);

-- Dimension Table: Dim_OrderPriority
CREATE TABLE if not exists gold_demo.default.Dim_OrderPriority (
    orderprioritykey BIGINT GENERATED ALWAYS AS IDENTITY,
    orderpriorityvalue STRING,
    last_added_dt_time TIMESTAMP
);

-- Dimension Table: Dim_Date
CREATE TABLE if not exists gold_demo.default.Dim_Date (
    datekey BIGINT,
    date DATE,
    year bigint,
    month bigint,
    day bigint,
    quarter bigint,
    week bigint
);

CREATE TABLE if not exists gold_demo.default.Dim_Supplier (
    suppkey INT,
    name STRING,
    address STRING,
    phone STRING,
    nationkey INT,
    acctbal FLOAT,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not exists gold_demo.default.Dim_Customer (
    custkey INT,
    name STRING,
    address STRING,
    phone STRING,
    nationkey INT,
    acctbal FLOAT,
    mktsegment STRING,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not exists gold_demo.default.Fact_Orders (
    orderkey INT,
    custkey INT,
    orderstatuskey INT,
    totalprice FLOAT,
    orderdatekey INT,
    orderprioritykey INT,
    shippriority INT,
    last_added_dt_time TIMESTAMP
);

CREATE TABLE if not exists gold_demo.default.Fact_LineItem (
    orderkey BIGINT,
    partkey BIGINT,
    suppkey BIGINT,
    quantity INT,
    retailprice FLOAT,
    extendedprice FLOAT,
    discountamount FLOAT,
    taxamount FLOAT,
    price_after_discount FLOAT,
    price_after_disc_tax FLOAT,
    returnflagkey INT,
    shipdatekey INT,
    commitdatekey INT,
    receiptdatekey INT,
    shipmodekey INT,
    shipinstructkey INT,
    last_added_dt_time TIMESTAMP
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Loading the tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.dim_part').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('insert into gold_demo.default.dim_part select partkey ,\
-- MAGIC     name ,\
-- MAGIC     mfgr ,\
-- MAGIC     brand ,\
-- MAGIC     type ,\
-- MAGIC     size ,\
-- MAGIC     container ,\
-- MAGIC     retailprice ,\
-- MAGIC     current_timestamp() \
-- MAGIC     from silver_demo.default.part')
-- MAGIC else:
-- MAGIC     spark.sql('insert into gold_demo.default.dim_part select partkey ,\
-- MAGIC     name ,\
-- MAGIC     mfgr ,\
-- MAGIC     brand ,\
-- MAGIC     type ,\
-- MAGIC     size ,\
-- MAGIC     container ,\
-- MAGIC     retailprice ,\
-- MAGIC     current_timestamp() from silver_demo.default.part \
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.dim_part)')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count = spark.sql('select count(*) from gold_demo.default.dim_nation').collect()[0][0]
-- MAGIC if tot_count == 0:
-- MAGIC     spark.sql('''insert into gold_demo.default.dim_nation
-- MAGIC                 select n.nationkey, n.name, r.name, current_timestamp()
-- MAGIC                 from silver_demo.default.nation n 
-- MAGIC                 join silver_demo.default.region r on n.region = r.regionkey''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.dim_nation
-- MAGIC                 select n.nationkey, n.name, r.name, current_timestamp()
-- MAGIC                 from silver_demo.default.nation n 
-- MAGIC                 join silver_demo.default.region r on n.region = r.regionkey
-- MAGIC                 where n.last_added_dt_time > (select max(last_added_dt_time) from gold_demo.default.dim_nation)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_Shipmode').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.dim_shipmode (
-- MAGIC     shipmodevalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct shipmode,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_Shipmode (
-- MAGIC     shipmodevalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct shipmode,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_Shipmode)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_ShipInstruct').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_ShipInstruct (
-- MAGIC     shipinstructvalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct shipinstruct,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_ShipInstruct (
-- MAGIC     shipinstructvalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct shipinstruct,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_ShipInstruct)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_ReturnFlag').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_ReturnFlag (
-- MAGIC     returnflagvalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct returnflag,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_ReturnFlag (
-- MAGIC     returnflagvalue ,
-- MAGIC     last_added_dt_time)
-- MAGIC     select distinct returnflag,current_timestamp()
-- MAGIC     from silver_demo.default.lineitem
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_ReturnFlag)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_OrderStatus').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_OrderStatus (
-- MAGIC     orderstatusvalue ,
-- MAGIC     last_added_dt_time )
-- MAGIC     select distinct orderstatus,current_timestamp()
-- MAGIC     from silver_demo.default.orders''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_OrderStatus (
-- MAGIC     orderstatusvalue ,
-- MAGIC     last_added_dt_time )
-- MAGIC     select distinct orderstatus,current_timestamp()
-- MAGIC     from silver_demo.default.orders
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_OrderStatus)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_OrderPriority').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_OrderPriority (
-- MAGIC     orderpriorityvalue ,
-- MAGIC     last_added_dt_time )
-- MAGIC     select distinct orderpriority,current_timestamp()
-- MAGIC     from silver_demo.default.orders''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_OrderPriority (
-- MAGIC     orderpriorityvalue ,
-- MAGIC     last_added_dt_time )
-- MAGIC     select distinct orderpriority,current_timestamp()
-- MAGIC     from silver_demo.default.orders
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_OrderPriority)''')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_Supplier').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_Supplier select suppkey ,
-- MAGIC     name ,
-- MAGIC     address ,
-- MAGIC     phone,
-- MAGIC     nationkey ,
-- MAGIC     acctbal ,
-- MAGIC     current_timestamp() 
-- MAGIC     from silver_demo.default.supplier''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_Supplier select suppkey,
-- MAGIC     name ,
-- MAGIC     address ,
-- MAGIC     phone,
-- MAGIC     nationkey ,
-- MAGIC     acctbal ,
-- MAGIC     current_timestamp() from silver_demo.default.supplier 
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_Supplier)''')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.Dim_Customer').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_Customer select customerkey ,
-- MAGIC     name ,
-- MAGIC     address ,
-- MAGIC     phone,
-- MAGIC     nationkey ,
-- MAGIC     acctbal ,
-- MAGIC     mktsegment ,
-- MAGIC     current_timestamp()
-- MAGIC     from silver_demo.default.customer''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.Dim_Customer select  customerkey ,
-- MAGIC     name ,
-- MAGIC     address ,
-- MAGIC     phone
-- MAGIC     nationkey ,
-- MAGIC     acctbal ,
-- MAGIC     mktsegment ,
-- MAGIC     current_timestamp() from silver_demo.default.customer \
-- MAGIC     where last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.Dim_Customer)''')
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta
-- MAGIC from math import ceil
-- MAGIC start_date = datetime(1991, 1, 1)
-- MAGIC end_date = datetime(1999, 12, 31)
-- MAGIC
-- MAGIC # Define list of dates
-- MAGIC dates = []
-- MAGIC
-- MAGIC # Define delta of time to advance on each loop
-- MAGIC delta = timedelta(days=1)
-- MAGIC loop_date = start_date
-- MAGIC
-- MAGIC # Loop to generate dates
-- MAGIC while loop_date <= end_date:
-- MAGIC     # Create a dictionary representing each date with its attributes
-- MAGIC     week_number = loop_date.isocalendar()[1]
-- MAGIC     row = {
-- MAGIC         "datekey":int(str(loop_date.year)+str(loop_date.month)+str(loop_date.day)),
-- MAGIC         "date": loop_date.date(),
-- MAGIC         "year": loop_date.year,
-- MAGIC         "month": loop_date.month,
-- MAGIC         "day": loop_date.day,
-- MAGIC         "quarter": ceil(loop_date.month / 3),
-- MAGIC         "week":week_number
-- MAGIC     }
-- MAGIC     # Append the dictionary to the list of dates
-- MAGIC     dates.append(row)
-- MAGIC     # Increment loop_date by delta
-- MAGIC     loop_date += delta
-- MAGIC
-- MAGIC # Create a DataFrame from the list of dates
-- MAGIC dates_df = spark.createDataFrame(dates)
-- MAGIC
-- MAGIC
-- MAGIC # dates_df = dates_df.withColumnRenamed("datekey", "new_datekey").withColumnRenamed("year", "new_year"). withColumnRenamed("month", "new_month").withColumnRenamed("day", "new_day").withColumnRenamed("quarter", "new_quarter").withColumnRenamed("date", "new_date").withColumnRenamed("week", "new_week")
-- MAGIC
-- MAGIC new_column_order = ['datekey' ,'date' ,'year' ,'month' ,'day' ,'quarter' ,'week' ]
-- MAGIC # Show the DataFrame
-- MAGIC dates_df=dates_df.select(new_column_order)
-- MAGIC display(dates_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dates_df.write.mode("overwrite").saveAsTable("gold_demo.default.Dim_date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tot_count=spark.sql('select count(*) from gold_demo.default.fact_lineitem').collect()[0][0]
-- MAGIC if tot_count==0:
-- MAGIC     spark.sql('''insert into gold_demo.default.fact_lineitem select 
-- MAGIC     orderkey,
-- MAGIC     l.partkey ,
-- MAGIC     suppkey ,
-- MAGIC     quantity ,
-- MAGIC     p.retailprice,
-- MAGIC     extendedprice ,
-- MAGIC     extendedprice*discount as discountamount ,
-- MAGIC     extendedprice-(extendedprice*discount)*tax as taxamount,
-- MAGIC     extendedprice-(extendedprice*discount) as price_after_discount,
-- MAGIC     extendedprice-(extendedprice*discount)+(extendedprice+((extendedprice-(extendedprice*discount))*tax))  as price_after_disc_tax,
-- MAGIC     r.returnflagkey as returnflagkey ,
-- MAGIC     d1.datekey as shipdatekey ,
-- MAGIC     d2.datekey as commitdatekey,
-- MAGIC     d3.datekey  as receiptdatekey,
-- MAGIC     sm.shipmodekey as shipmodekey,
-- MAGIC     si.shipinstructkey as shipinstructkey,
-- MAGIC     current_timestamp()
-- MAGIC     from silver_demo.default.lineitem l join gold_demo.default.dim_part p
-- MAGIC     on l.partkey=p.partkey
-- MAGIC     join gold_demo.default.dim_returnflag r
-- MAGIC     on l.returnflag=r.returnflagvalue
-- MAGIC     join gold_demo.default.dim_date d1
-- MAGIC     on l.shipdate=d1.date
-- MAGIC     join gold_demo.default.dim_date d2
-- MAGIC     on l.commitdate=d2.date
-- MAGIC     join gold_demo.default.dim_date d3
-- MAGIC     on l.receiptdate=d3.date
-- MAGIC     join gold_demo.default.dim_shipinstruct si
-- MAGIC     on l.shipinstruct=si.shipinstructvalue
-- MAGIC     join gold_demo.default.dim_shipmode sm
-- MAGIC     on l.shipmode=sm.shipmodevalue ''')
-- MAGIC else:
-- MAGIC     spark.sql('''insert into gold_demo.default.fact_lineitem select  
-- MAGIC     orderkey,
-- MAGIC     l.partkey ,
-- MAGIC     suppkey ,
-- MAGIC     quantity ,
-- MAGIC     p.retailprice,
-- MAGIC     extendedprice ,
-- MAGIC     extendedprice*discount as discountamount ,
-- MAGIC     extendedprice-(extendedprice*discount)*tax as taxamount,
-- MAGIC     extendedprice-(extendedprice*discount) as price_after_discount,
-- MAGIC     extendedprice-(extendedprice*discount)+(extendedprice+((extendedprice-(extendedprice*discount))*tax)  as price_after_disc_tax,
-- MAGIC     r.returnflagkey as returnflagkey ,
-- MAGIC     d1.datekey as shipdatekey ,
-- MAGIC     d2.datekey as commitdatekey,
-- MAGIC     d3.datekey  as receiptdatekey,
-- MAGIC     sm.shipmodekey as shipmodekey,
-- MAGIC     si.shipinstructkey as shipinstructkey,
-- MAGIC     current_timestamp()
-- MAGIC     from silver_demo.default.lineitem l join gold_demo.default.dim_part p
-- MAGIC     on l.partkey=p.partkey
-- MAGIC     join gold_demo.default.dim_returnflag r
-- MAGIC     on l.returnflag=r.returnflagvalue
-- MAGIC     join gold_demo.default.dim_date d1
-- MAGIC     on l.shipdate=d1.date
-- MAGIC     join gold_demo.default.dim_date d2
-- MAGIC     on l.commitdate=d2.date
-- MAGIC     join gold_demo.default.dim_date d3
-- MAGIC     on l.receiptdate=d3.date
-- MAGIC     join gold_demo.default.dim_shipinstruct si
-- MAGIC     on l.shipinstruct=si.shipinstructvalue
-- MAGIC     join gold_demo.default.dim_shipmode sm
-- MAGIC     on l.shipmode=sm.shipmodevalue 
-- MAGIC     where l.last_added_dt_time> (select max(last_added_dt_time) from gold_demo.default.fact_lineitem)''')
-- MAGIC
