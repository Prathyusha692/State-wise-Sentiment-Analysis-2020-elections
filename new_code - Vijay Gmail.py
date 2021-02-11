# import sparkpickle
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql import DataFrame
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, Row, ArrayType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import *
import json
import os
from operator import add

broadcastStates=[]

def init_spark():
    spark = SparkSession.builder.appName("Twitter Analysis") \
        .config("spark.memory.fraction", 0.8) \
        .config("spark.sql.shuffle.partitions", "800") \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.binaryAsString", "true") \
        .getOrCreate()
    sc = spark.sparkContext
    return spark, sc

## Added UDf to Get max_state_code
def get_max_state_code(str):

    import builtins
    if builtins.max(str.items(), key = lambda k : k[0], default=0)[0] in str.keys():
        # print(builtins.max(str.items(), key=lambda k : k[0], default=0)[0])
        # print(builtins.max(str.items(), key=lambda k: k[0], default=0)[1])
        return builtins.max(str.items(), key=lambda k: k[0], default=0)[1]
    else:
        return ''


def get_state_code(str):
    words = str.split()
    grouped_words = [' '.join(words[i: i + 2]) for i in range(0, len(words), 1)] + words
    word_list = sorted(list(dict.fromkeys(grouped_words)), key=len)
    # print(word_list)
    states = []
    res = {}
    for i in broadcastStates[0]:
        for j in i:
            for name in word_list:
                if name in j['state_meta']:
                    resStr = j['state_code']
                    # print(resStr)
                    states.append(resStr)
        my_dict = {i:states.count(i) for i in states}
        for i, v in my_dict.items():
          res[v] = [i] if v not in res.keys() else res[v] + [i]
        if res:
            return res
        else:
            empty_res = {'1': ['No State']}
            return empty_res
        # return res

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def get_dir(path):
    file_paths = []
    for root, directories, files in os.walk(path, topdown=False):
        for name in files:
            file_paths.append(os.path.join(root, name))
    return file_paths

def main():
    geo_true_file_path = 'data/geo_true/'

    spark, sc = init_spark()
    ps = sc.wholeTextFiles(r"E:\Workspace\PycharmProjects\TwitterProject\latest_dev\us_state_meta.json").values().map(json.loads)
    # broadcastStates.append(ps.map(lambda x: x).collect())
    broadcastStates.append(spark.sparkContext.broadcast(ps.map(lambda x: x).collect()).value)
    geo_true_file_list = get_dir(geo_true_file_path)
    # print(geo_true_file_list)
    # geo_true_df = spark.read.parquet("/data/geo_true/")
    geo_true_df = spark.read.parquet(*geo_true_file_list)
    geo_true_df = geo_true_df.filter(geo_true_df.country_code == 'US')
    geo_true_df = geo_true_df.drop('_id', 'coordinates', 'country_code').withColumnRenamed("city_state", "location")
    geo_true_df = geo_true_df.withColumn("new_location", F.lower(F.col("location")))
    geo_true_df = geo_true_df.withColumn('new_location', regexp_replace('new_location', '[^a-zA-Z0-9_]+', ' '))
    geo_udf = udf(lambda x: get_state_code(x), StringType())
    geo_true_df = geo_true_df.withColumn('new_location', geo_udf('new_location'))
    geo_true_df.show(10)

    ## Added To get Max Key,value of State Codes
    geo_udf_1 = udf(lambda x: get_max_state_code(x), ArrayType(StringType()))
    geo_true_df = geo_true_df.withColumn('new_location_1', geo_udf_1('new_location'))
    geo_true_df.show(10)
    
    # Explode the Array
    geo_true_df.withColumn("new_location_2", explode(geo_true_df["new_location_1"])).show()

    # geo_true_df.select("location","new_location").distinct().coalesce(1).write.mode('overwrite').csv("new_location/")

if __name__ == '__main__':
    main()
    
