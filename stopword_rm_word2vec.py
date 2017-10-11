#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.feature import Word2Vec
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import format_number as fmt
sc = SparkContext('local')
spark = SparkSession(sc)
fieldSchema = StructType([StructField("urlid", LongType(), True),
  StructField("title", StringType(), True),
  StructField("s_title", StringType(), True),
  StructField("s_content", StringType(), True)
])


print "begin to map input"

train_set = sc.textFile("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_order/*")
print train_set.take(5)
def combine_uinfo(line):
  line_arr = line.split("\t", 4)
  arr_len = len(line_arr)
  if arr_len < 4:
      if arr_len == 3:
          s_term_arr = line_arr[2].split(",")
          return (line_arr[0], line_arr[1], s_term_arr)
      elif arr_len == 2:
          s_term_arr = [line_arr[1]]
          return (line_arr[0], line_arr[1], s_term_arr)
      else:
          return (line, "-", [])
      
  (urlid, title, s_title, s_content) = line.split("\t")

  s_term = s_title + "," + s_content
  s_term_arr = s_term.split(",")
  return (urlid, title, s_term_arr)


train_set_combine = train_set.map(combine_uinfo)
train_set_combine_d = spark.createDataFrame(train_set_combine, ['urlid', 'title', 's_term'])
train_set_combine_d.show()

remover = StopWordsRemover(inputCol="s_term", outputCol="s_term_filtered")
remover_res = remover.transform(train_set_combine_d)
remover_res.show()

remover_res.createOrReplaceTempView("remover_res")

sql_query = """
SELECT urlid, title, s_term_filtered
FROM remover_res
"""
remover_res_new = spark.sql(sql_query)

remover_res_new.rdd.saveAsTextFile("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_filtered.set")

#word2vec
word2Vec = Word2Vec(vectorSize=128, minCount=2, inputCol="s_term_filtered", outputCol="result")
model = word2Vec.fit(remover_res_new)
model_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/word2vec_model"
model.save(model_path)

model.getVectors().show(truncate=False)
model.findSynonyms("演艺圈", 5).select("word", fmt("similarity", 5).alias("similarity")).show()

result = model.transform(remover_res_new)
result.show(truncate=False)
result.rdd.saveAsTextFile("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_word2vec.set")

print "finish"
