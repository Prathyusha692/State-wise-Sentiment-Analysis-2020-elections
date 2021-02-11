# Databricks notebook source
str = "new york city"
words = str.split()
words

# COMMAND ----------

grouped_words = [' '.join(words[i: i + 2]) for i in range(0, len(words))] + words
grouped_words

# COMMAND ----------

 word_list = sorted(list(dict.fromkeys(grouped_words)), key=len)
 word_list

# COMMAND ----------

# import sparkpickle
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, Row, ArrayType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from operator import add
import json
spark = SparkSession.builder.appName("Twitter Analysis") \
        .config("spark.memory.fraction", 0.8) \
        .config("spark.sql.shuffle.partitions", "800") \
        .getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

broadcastStates=[]

# COMMAND ----------

ps = sc.wholeTextFiles(r"/FileStore/tables/tarun_latest/us_state_meta.json").values().map(json.loads)
broadcastStates.append(spark.sparkContext.broadcast(ps.map(lambda x: x).collect()).value)
print(broadcastStates)

# COMMAND ----------

def get_state_code(str):
    words = str.split()
    grouped_words = [' '.join(words[i: i + 2]) for i in range(0, len(words), 1)] + words
    word_list = sorted(list(dict.fromkeys(grouped_words)), key=len)
#     print(word_list)
    state=[]
    for i in broadcastStates[0]:
        for j in i:
#             print(j['state_code'])
#             print(j['state_meta'])
            for name in word_list:
                if name in j['state_meta']:
                    resStr = j['state_code']
                    state.append(resStr)
#                     print(name)
#                     return resStr
    print(state)
    df =sc.parallelize(state).map(lambda word: (word, 1)).reduceByKey(add).collect()
    if(len(df)==1):
      return df[0][0]
    else:
      return df

    return None

# COMMAND ----------

get_state_code("york")

# COMMAND ----------

# spark, sc = init_spark()
ps = sc.wholeTextFiles(r"/FileStore/tables/tarun_latest/us_state_meta.json").values().map(json.loads)
broadcastStates.append(spark.sparkContext.broadcast(ps.map(lambda x: x).collect()).value)
# broadcastStates.append(ps.map(lambda x: x).collect())
# lat_lon = df.rdd.map(lambda x : [x.latitude, x.longitude]).collect(
print((broadcastStates))

# COMMAND ----------


