# Databricks notebook source
#Import Commands
####################################################
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, Row, ArrayType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import json,re


# COMMAND ----------

#Create Spark Session and load US State Metadata UDF
####################################################
spark = SparkSession.builder.appName("Twitter Analysis").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

broadcastStates=[]
ps = sc.wholeTextFiles("/FileStore/tables/us_state_meta_latest.json").values().map(json.loads)
broadcastStates.append(spark.sparkContext.broadcast(ps.map(lambda x: x).collect()).value)

def get_new_us_code(str):
    states =["ak","al","az","ar","ca","co","ct","dc","de","fl","ga","hi","id","il","in","ia","ks","ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny","nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy"]   
    words = re.sub('[^a-zA-Z\']+', ' ', str).lower().split()
    grouped_words = [' '.join(words[i: i + 2]) for i in range(0, len(words), 1)] + words
    word_list = sorted(list(dict.fromkeys(grouped_words)), key=len)
#    print(word_list)

    for i in broadcastStates[0]:
        for j in i:
#             print(j['country_code'])
#             print(j['comments'])
            for name in word_list:
                if name in states:
                    return name
                if name in j['comments']:
                    resStr = j['country_code']
                    print(resStr)
                    return resStr

    return None

# COMMAND ----------

#Read geo_true and geo_false data
geo_false_df=spark.read.parquet("/FileStore/tables/geo_false")
print("Geo False count : "+str(geo_false_df.count()))
geo_true_df=spark.read.parquet("/FileStore/tables/geo_true")
print("Geo True count : "+str(geo_true_df.count()))
#Drop Columns & rename
geo_false_df = geo_false_df.drop('_id','id','lang')
geo_true_df = geo_true_df.filter(geo_true_df.country_code == 'US')
geo_true_df = geo_true_df.drop('_id','id','coordinates', 'country_code').withColumnRenamed("city_state", "location")

# COMMAND ----------

#Drop location null rows
geo_false_df=geo_false_df.where(col("location").isNotNull())
geo_true_df=geo_true_df.where(col("location").isNotNull())
print("Geo False after dropping null count : "+str(geo_false_df.count()))
print("Geo True after dropping null count : "+str(geo_true_df.count()))

#Get state code from location
geo_udf = udf(lambda x: get_new_us_code(x), StringType())
geo_true_df = geo_true_df.withColumn('new_location', F.upper(geo_udf('location'))).where(col("new_location").isNotNull())
geo_true_df.write.mode('overwrite').parquet("staging/geo_true")


geo_false_df = geo_false_df.withColumn('new_location', F.upper(geo_udf('location'))).where(col("new_location").isNotNull())
geo_false_df.write.mode('overwrite').parquet("staging/geo_false")




# COMMAND ----------

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

  tweets = unionAll(geo_true_df, geo_false_df).distinct()

# COMMAND ----------

df=spark.read.parquet("/staging/geo_false/")
df.printSchema()
print(df.count())
df.show()

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/tagged tweets/",True)

# COMMAND ----------


