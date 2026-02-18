# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.nycstoragerohan.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nycstoragerohan.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nycstoragerohan.dfs.core.windows.net", "f105c89f-5741-4385-9113-8f038715eb51")
spark.conf.set("fs.azure.account.oauth2.client.secret.nycstoragerohan.dfs.core.windows.net", ".ez8Q~XVgEoLj5UjMSk.MlN~pSaDphdCG0fDybNF")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nycstoragerohan.dfs.core.windows.net", "https://login.microsoftonline.com/8b9f4eaf-a4e8-42a6-b847-58903c75b282/oauth2/token")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold
# MAGIC -- drop database gold

# COMMAND ----------

silver = 'abfss://silver@nycstoragerohan.dfs.core.windows.net'
gold = 'abfss://gold@nycstoragerohan.dfs.core.windows.net'

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                    .option('inferSchema', True)\
                    .option('header', True)\
                    .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format("delta") \
    .mode("append") \
    .save(f"{gold}/trip_zone")

# COMMAND ----------

df_trip_type = spark.read.format('parquet')\
                         .option('inferSchema', True)\
                         .option('header', True)\
                         .load(f'{silver}/trip_type')

# COMMAND ----------

df_trip_type.write.format('delta')\
                  .mode('append')\
                  .save(f'{gold}/trip_type')

# COMMAND ----------

df_trip_data = spark.read.format('parquet')\
                         .option('inferSchema', True)\
                         .option('header', True)\
                         .load(f'{silver}/tripsdata2023')

# COMMAND ----------

df_trip_type.write.format('delta')\
        .mode('append')\
        .save(f'{gold}/tripsdata2023')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone`
# MAGIC where locationid = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC update delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone`
# MAGIC set borough = 'EMR'
# MAGIC where LocationId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone`

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone`
# MAGIC where locationId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone` version as of 1
# MAGIC where locationid = 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time Travel to updated value of locationid = 1 table

# COMMAND ----------

# MAGIC %sql
# MAGIC restore delta.`abfss://gold@nycstoragerohan.dfs.core.windows.net/trip_zone` to version as of 0