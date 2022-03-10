# Databricks notebook source
# MAGIC %md
# MAGIC ######Intializing credentials through Secrets Scope via Azure Key Vault

# COMMAND ----------

storage_account_name = "formula1mohit"
client_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-secret")
ff

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

## Azure DL Mount to DBFS Function
def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

## Calling Function with azure container name 
mount_adls("raw")


# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/mnt/formula1mohit/raw/

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1mohit/")

# COMMAND ----------

# MAGIC %fs 
# MAGIC head dbfs:/mnt/formula1mohit/raw/circuits.csv

# COMMAND ----------

# Creating dataframe through csv file
circuits_df = spark.read \
.option("header", True) \
.csv("/mnt/formula1mohit/raw/circuits.csv")

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC --Creating Dummy table
# MAGIC create table  temp_data_3 using CSV OPTIONS (path="/mnt/formula1mohit/raw/circuits.csv", header="true")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from temp_data_3;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  src_tbl
# MAGIC      ( src_col_value_1  varchar(20)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  tgt_tbl
# MAGIC      ( tgt_col_value_1  varchar(20)) 

# COMMAND ----------



var_a= "CREATE OR REPLACE TABLE  Metadata_Mapping_table ( src_tbl string , src_col string, tgt_tbl string, tgt_col string)"

# COMMAND ----------

print(var_a)

# COMMAND ----------

import pyspark.sql 

# COMMAND ----------

# Running SQL commands through spark SQL API
res=spark.sql(var_a)

# COMMAND ----------

res.show()

# COMMAND ----------

insert_var= "insert into Metadata_Mapping_table values('Party','Party_name','Target_Party','target_party_name')"

# COMMAND ----------

res_1=spark.sql(insert_var)

# COMMAND ----------

res_1.show()

# COMMAND ----------

select_var="select * from Metadata_Mapping_table"

# COMMAND ----------

spark.sql(select_var).show()

# COMMAND ----------


multi_var=["insert into tgt_tbl values('tgt_Cust_id1');"," insert into tgt_tbl values('tgt_Party_id1');"]

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tgt_tbl;

# COMMAND ----------

li=[]
for x in multi_var:
      li.append(spark.sql(x))

# COMMAND ----------

li[0].show()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  Customer
# MAGIC      ( Cust_id int ,Cust_name  varchar(20)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  Target_customer
# MAGIC      ( target_cust_id int ,target_cust_name  varchar(20)) 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC insert into Customer values(101, 'XYZ');
# MAGIC insert into Customer values (102,'ABC');

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Customer;

# COMMAND ----------

df = sqlContext.table("Metadata_Mapping_table")

# COMMAND ----------

df.show()

# COMMAND ----------

for i in df.collect():
    print(i['src_col'] ,i['tgt_col'])

# COMMAND ----------


tgt_ls=df.select('tgt_tbl').distinct().collect()

# COMMAND ----------

tgt_ls[0]['tgt_tbl']

# COMMAND ----------

df.filter("tgt_tbl='Target_customer'").show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ###Loading data into Target Table using Source table via Metadata table

# COMMAND ----------

'''
1. Storing distinct target table list into a python list.
2. Creation of temp DF by filtering only 1 target table at a time.
3. Then iterating through all the rows of temp DF and creating SQL insert queries.
'''


tgt_ls=df.select('tgt_tbl').distinct().collect()
hit_query=[]
for item in tgt_ls:
    temp=("tgt_tbl="+"'"+str(item['tgt_tbl'])+"'")
    temp_df=df.filter(temp).collect()
    target_cols=" "
    source_queries=" "
    for row in temp_df:
        target_cols= target_cols+ row['tgt_col'] +","
        source_queries=source_queries+ row['src_col'] +","
    hit_query.append("insert into "+ item['tgt_tbl'] +"("+ target_cols[0:len(target_cols)-1] +")" + " select "+ source_queries[0:len(source_queries)-1]  + " from " + row['src_tbl']    +" ;")
  

# COMMAND ----------

hit_query

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from target_customer;
# MAGIC -- insert into target_customer( target_cust_name,target_cust_id) select  Cust_name,Cust_id from customer ;
# MAGIC -- insert into target_customer( target_cust_name, target_cust_id)  (select Cust_name from customer UNION select Cust_id from customer) ;

# COMMAND ----------

import pyspark.sql.functions as f

df_tgt=df.groupBy("tgt_tbl").pivot("tgt_col").agg(f.concat_ws(", ", f.collect_list(df.tgt_col))).collect()

# COMMAND ----------

df_tgt.show()

# COMMAND ----------

insert_meta=[]
for r in df_tgt.collect():
    print(r)
    
#for row in df.collect():
 #   insert_meta.append("insert into "+ row['tgt_tbl'] +" values( select "+ row['src_col'] + " from " + row['src_tbl'] +" );")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE test_db 
# MAGIC location "dbfs:/mnt/formula1mohit/demo/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE test_db 
# MAGIC location "dbfs:/FileStore/"

# COMMAND ----------

demo_df=spark.sql("select * from target_customer")
demo_src=spark.sql("select * from customer")

# COMMAND ----------

demo_df.write.format("delta").save("dbfs:/FileStore/")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "dbfs:/mnt/formula1mohit/demo/demo_table"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_db.demo_table;

# COMMAND ----------

tt=demo_df.schema.names
tt
xx

# COMMAND ----------

from pyspark.sql.functions import concat_ws,col,lit
aa=[]
xx=demo_src.select(demo_src.Cust_id,demo_src.Cust_name).rdd.map(lambda x: (x)).collect()

# COMMAND ----------

df_11=spark.createDataFrame(data=xx,schema=["target_cust_id", "target_cust_name"])

# COMMAND ----------

df_11.show()

# COMMAND ----------

demo_df.union(df_11).show()

# COMMAND ----------

