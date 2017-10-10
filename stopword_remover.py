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
sc = SparkContext('local')
spark = SparkSession(sc)
fieldSchema = StructType([StructField("urlid", LongType(), True),
  StructField("title", StringType(), True),
  StructField("s_title", StringType(), True),
  StructField("s_content", StringType(), True)
])


print "begin to map input"
train_set = spark.read.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term.set", schema=fieldSchema)

def combine_uinfo(line):
  s_term = line.s_title + line.s_content
  return (line.urlid, line.title, s_term)

train_set_combine = train_set.rdd.map(combine_uinfo)
train_set_combine_d = spark.createDataFrame(train_set_combine, ['urlid', 'title', 's_term'])
train_set_combine_d.show()

remover = StopWordsRemover(inputCol="s_term", outputCol="s_term_filtered")
remover_res = remover.transform(sentenceData)
remover_res.show(truncate=False)

print "finish"

