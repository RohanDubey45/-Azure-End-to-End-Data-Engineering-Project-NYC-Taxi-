# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Access

# COMMAND ----------

appid = "f105c89f-5741-4385-9113-8f038715eb51"
dirid = " 8b9f4eaf-a4e8-42a6-b847-58903c75b282 "

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.nycstoragerohan.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nycstoragerohan.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nycstoragerohan.dfs.core.windows.net", "f105c89f-5741-4385-9113-8f038715eb51")
spark.conf.set("fs.azure.account.oauth2.client.secret.nycstoragerohan.dfs.core.windows.net", ".ez8Q~XVgEoLj5UjMSk.MlN~pSaDphdCG0fDybNF")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nycstoragerohan.dfs.core.windows.net", "https://login.microsoftonline.com/8b9f4eaf-a4e8-42a6-b847-58903c75b282/oauth2/token")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nycstoragerohan.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Trip Type & Trip Zone Data

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                    .option('inferSchema', True)\
                    .option('header', True)\
                    .load('abfss://bronze@nycstoragerohan.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                         .option('header', True)\
                         .option('inferSchema', True)\
                         .load('abfss://bronze@nycstoragerohan.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading Trip Data

# COMMAND ----------

mySchema = """
VendorID BIGINT,
lpep_pickup_datetime timestamp,
lpep_dropoff_datetime timestamp,
store_and_fwd_flag STRING,
RatecodeID BIGINT,
PULocationID BIGINT,
DOLocationID BIGINT,
passenger_count BIGINT,
trip_distance DOUBLE,
fare_amount DOUBLE,
extra DOUBLE,
mta_tax DOUBLE,
tip_amount DOUBLE,
tolls_amount DOUBLE,
ehail_fee DOUBLE,
improvement_surcharge DOUBLE,
total_amount DOUBLE,
payment_type BIGINT,
trip_type BIGINT,
congestion_surcharge DOUBLE
"""


# COMMAND ----------

df_trip = spark.read.format('parquet')\
                    .schema(mySchema)\
                    .option('header', True)\
                    .option('recursiveFileLookup', True)\
                    .load('abfss://bronze@nycstoragerohan.dfs.core.windows.net/tripsdata2023')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformation

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description', 'trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
                  .mode('append')\
                  .option('path', 'abfss://silver@nycstoragerohan.dfs.core.windows.net/trip_type')\
                  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip Zone Dfn

# COMMAND ----------

df_trip_zone =  df_trip_zone.withColumn(
    'zone-1',
    split(
        col('Zone'),
        '/'
    )[0]
)\
.withColumn(
    'zone-2',
    split(
        col('Zone'),
        '/'
    )[1]
)

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
                  .mode('append')\
                  .option('path', 'abfss://silver@nycstoragerohan.dfs.core.windows.net/trip_zone')\
                  .save()

# COMMAND ----------

df_trip = df_trip.withColumn(
                'date',
                to_date(col('lpep_pickup_datetime'))
        )\
        .withColumn(
            'year',
            year(col('lpep_pickup_datetime'))
        )\
        .withColumn(
            'month',
            month(col('lpep_pickup_datetime'))
        )



# COMMAND ----------

df_trip.display()

# COMMAND ----------

# DBTITLE 1,Cell 24
df_trip = df_trip.select('VendorID', 'PULocationID', 'DOLocationID', 'fare_amount', 'total_amount')
df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
             .mode('append')\
             .option('path', 'abfss://silver@nycstoragerohan.dfs.core.windows.net/tripsdata2023')\
             .save()
                 
