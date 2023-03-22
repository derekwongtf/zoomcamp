#!/usr/bin/env python
# coding: utf-8

# In[8]:
# how to run this script
# URL=spark://DavidNg-8242-NB.:7077
# spark-submit \
#     --master="${URL}" \
#     06_spark_sql_cluster.py \
#       --input_green=../week5/code/data/pq/green/2021/*/ \
#       --input_yellow=../week5/code/data/pq/yellow/2021/*/ \
#       --output=../week5/code/data/report-2021

# python 06_spark_sql_cluster.py \
#     --input_green=../week5/code/data/pq/green/2020/*/ \
#     --input_yellow=../week5/code/data/pq/yellow/2020/*/ \
#     --output=../week5/code/data/report-2020

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

# spark = SparkSession.builder \
#     .master("spark://DavidNg-8242-NB.:7077") \
#     .appName('test') \
#     .getOrCreate()

spark = SparkSession.builder \
     .appName('test') \
     .getOrCreate()

# In[5]:


df_green = spark.read.parquet(input_green)


# In[6]:


df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


# In[9]:


df_yellow = spark.read.parquet(input_yellow)


# In[10]:


df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')


# In[11]:


common_colums = []

yellow_columns = set(df_yellow.columns)

for col in df_green.columns:
    if col in yellow_columns:
        common_colums.append(col)


# In[17]:


common_colums


# In[12]:





# In[13]:


df_green_sel = df_green \
    .select(common_colums) \
    .withColumn('service_type', F.lit('green'))


# In[14]:


df_yellow_sel = df_yellow \
    .select(common_colums) \
    .withColumn('service_type', F.lit('yellow'))


# In[15]:


df_trips_data = df_green_sel.unionAll(df_yellow_sel)


# In[16]:


df_trips_data.groupBy('service_type').count().show()


# In[18]:


df_trips_data.columns


# In[19]:


df_trips_data.registerTempTable('trips_data')


# In[20]:


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


# In[ ]:


df_result.coalesce(1).write.parquet(output, mode='overwrite')

