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
gt_df = spark.read.parquet("/FileStore/tables/tarun_latest/geo_true/")geo_true_df = geo_true_df.filter(geo_true_df.country_code == 'US')
    geo_true_df = geo_true_df.drop('_id', 'coordinates', 'country_code').withColumnRenamed("city_state", "location")
    geo_true_df = geo_true_df.withColumn("new_location", F.lower(F.col("location")))
    geo_true_df = geo_true_df.withColumn('new_location', regexp_replace('new_location', '[^a-zA-Z0-9_]+', ' '))

# COMMAND ----------

gt_df = gt_df.filter(gt_df.country_code == 'US')
gt_df = gt_df.drop('_id', 'coordinates', 'country_code').withColumnRenamed("city_state", "location")
gt_df = gt_df.withColumn("new_location", F.lower(F.col("location")))
gt_df = gt_df.withColumn('new_location', regexp_replace('new_location', '[^a-zA-Z0-9_]+', ' '))
gt_df.show()

# COMMAND ----------



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
#     print(state)
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


# COMMAND ----------

def get_state_abbr(x):
    if re.match('({})'.format("|".join(two_word_states)), x.lower()):
        tokens = [re.match('({})'.format("|".join(two_word_states)), x.lower()).group(0)]
    elif re.match('({})'.format("|".join(city_to_state_dict.keys()).lower()), x.lower()):
        k = re.match('({})'.format("|".join(city_to_state_dict.keys()).lower()), x.lower()).group(0)
        tokens = [city_to_state_dict.get(k.title(), np.nan)]
    else:
        tokens = [j for j in re.split("\s|,", x) if j not in ['in', 'la', 'me', 'oh', 'or']]
    for i in tokens:
        if re.match('\w+', str(i)):
            if us.states.lookup(str(i)):
                return us.states.lookup(str(i)).abbr

# COMMAND ----------

states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New York', 'New Mexico', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
stateCodes = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
stateMapping = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 
                  'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NY': 'New York', 'NM': 'New Mexico', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT':  'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV':  'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}
#tweet_copied_df = tweet_df
for index, row in gt_df.iterrows():
  flag = 0
  if row.new_location:
    locationSplit = row.location.split(',')
    for word in locationSplit:
      word_stripped = word.strip()
      if word_stripped in states:
        flag = 1
        row['state'] = word_stripped
      elif word_stripped in stateCodes:
        flag = 1
        row['state'] = stateMapping[word_stripped]
  if flag == 0:
    tweet_copied_df = tweet_copied_df.drop(index=index)
  else:
    tweet_copied_df.loc[index, 'state'] = row['state']

# COMMAND ----------


