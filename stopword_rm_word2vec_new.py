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
fieldSchema = StructType([StructField("urlid", StringType(), True),
  StructField("title", StringType(), True),
  StructField("s_term_filtered", StringType(), True)
])
print "begin to map input"
raw_data = spark.read.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_filtered/part-*", schema=fieldSchema)

def combine_uinfo(line):
  if line.s_term_filtered is None:
    return (line.urlid, "-", [])
  term_arr = line.s_term_filtered.split(',')
  return (line.urlid, line.title, term_arr)

raw_data_map = raw_data.rdd.map(combine_uinfo)

fieldSchema1 = StructType([StructField("urlid", StringType(), True),
  StructField("title", StringType(), True),
  StructField("s_term", ArrayType(StringType()), True)
])

raw_data_d = spark.createDataFrame(raw_data_map, schema=fieldSchema1)

stop_word = sc.textFile("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/stop_word_uniq").collect()


remover = StopWordsRemover(inputCol="s_term", outputCol="s_term_filtered", stopWords=stop_word)
remover_res = remover.transform(raw_data_d)
remover_res.show()
remover_res.createOrReplaceTempView("remover_res")
sql_query = """
SELECT urlid, title, s_term_filtered
FROM remover_res
"""
remover_res_new_raw = spark.sql(sql_query)

def combine_term(line):
  term_combine = ""
  for i in range(0, len(line.s_term_filtered)):
    if len(term_combine) > 0:
      term_combine += ","
    term_combine += line.s_term_filtered[i]
  return (line.urlid, line.title, term_combine)

remover_res_write = remover_res_new_raw.rdd.map(combine_term)

fieldSchema2 = StructType([StructField("urlid", StringType(), True),
  StructField("title", StringType(), True),
  StructField("s_term_filtered", StringType(), True)
])

remover_res_write_d = spark.createDataFrame(remover_res_write, schema=fieldSchema2)
remover_res_write_d.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_filtered_new")

def truncate_term(line):
  if len(line.s_term_filtered) > 10:
    return (line.urlid, line.title, line.s_term_filtered[:10])
  return (line.urlid, line.title, line.s_term_filtered)
remover_res_new_map = remover_res_new_raw.rdd.map(truncate_term)

fieldSchema3 = StructType([StructField("urlid", StringType(), True),
  StructField("title", StringType(), True),
  StructField("s_term_filtered", ArrayType(StringType()), True)
])

remover_res_new = spark.createDataFrame(remover_res_new_map, schema=fieldSchema3)

#word2vec
word2Vec = Word2Vec(vectorSize=16, minCount=2, inputCol="s_term_filtered", outputCol="result")
model = word2Vec.fit(remover_res_new)
model_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/word2vec_model"
model.save(model_path)
model.getVectors().show(truncate=False)
model.findSynonyms("演艺圈", 5).select("word", fmt("similarity", 5).alias("similarity")).show()
result = model.transform(remover_res_new)
result.show()

def combine_term_word2vec(line):
  term_combine = ""
  for i in range(0, len(line.s_term_filtered)):
    if len(term_combine) > 0:
      term_combine += ","
    term_combine += line.s_term_filtered[i]

  res_combine = ""
  for i in range(0, len(line.result)):
    if len(res_combine) > 0:
      res_combine += ","
    res_combine += line.result[i]
  return (line.urlid, line.title, term_combine, res_combine)

w2v_res_write = result.rdd.map(combine_term_word2vec)
w2v_res_write_d = spark.createDataFrame(w2v_res_write, ['urlid', 'title', 's_term_filtered', 'result'])
w2v_res_write_d.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_word2vec")


print "finish"

