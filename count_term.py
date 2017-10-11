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
from operator import add
sc = SparkContext('local')
spark = SparkSession(sc)

fieldSchema = StructType([StructField("urlid", StringType(), True),
  StructField("title", StringType(), True),
  StructField("s_term_filtered", StringType(), True)
])
print "begin to map input"
raw_data = spark.read.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_filtered/part-*", schema=fieldSchema)

def combine_term(line):
  if line.s_term_filtered is None:
    s = "-" + "\a" + "0"
    return (line.urlid, line.title, s)
  term_arr = line.s_term_filtered.split(',')
  term_dict = dict()
  for i in range(0, len(term_arr)):
    if term_dict.has_key(term_arr[i]):
      term_dict[term_arr[i]] += 1
    else:
      term_dict[term_arr[i]] = 1
  s = ""
  for k,v in term_dict.items():
    if len(s) > 0:
      s += "\t"
    s = k + "\a" + str(v)
  return (line.urlid, line.title, s)

raw_data_map = raw_data.rdd.map(combine_term)

fieldSchema1 = StructType([StructField("term", StringType(), True)
])

raw_data_d = spark.createDataFrame(raw_data_map, schema=fieldSchema)
raw_data_d.show()
raw_data_d.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/term_doc_count")


word_count = raw_data_map.flatMap(lambda x: x.term.split('\t')).map(lambda x: x.split('\a')).map(lambda p: Row(p[0], int(p[1]))).reduceByKey(add)

fieldSchema2 = StructType([StructField("term", StringType(), True),
  StructField("count", IntegerType(), True)
])

word_count_d = spark.createDataFrame(word_count, schema=fieldSchema2)
word_count_d.show()
word_count_d.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/term_total_count")

raw_data_d.createOrReplaceTempView("td_count")
word_count_d.createOrReplaceTempView("t_count")


print "finish"

