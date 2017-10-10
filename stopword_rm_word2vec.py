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
train_set = spark.read.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_order/*", schema=fieldSchema)

def combine_uinfo(line):
  s_term = line.s_title + line.s_content
  return (line.urlid, line.title, s_term)

train_set_combine = train_set.rdd.map(combine_uinfo)
train_set_combine_d = spark.createDataFrame(train_set_combine, ['urlid', 'title', 's_term'])
train_set_combine_d.show()

remover = StopWordsRemover(inputCol="s_term", outputCol="s_term_filtered")
remover_res = remover.transform(sentenceData)
remover_res.show(truncate=False)

remover_res.createOrReplaceTempView("remover_res")

sql_query = """
SELECT urlid, title, s_term_filtered
FROM remover_res
"""
remover_res_new = spark.sql(sql_query)

remover_res_new.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_filtered.set")

#word2vec
word2Vec = Word2Vec(vectorSize=128, minCount=2, inputCol="s_term_filtered", outputCol="result")
model = word2Vec.fit(remover_res_new)
model_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/word2vec_model"
model.save(model_path)

model.getVectors().show(truncate=False)
model.findSynonyms("演艺圈", 5).select("word", fmt("similarity", 5).alias("similarity")).show()

result = model.transform(remover_res_new)
result.show(truncate=False)
result.write.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_word2vec.set")

print "finish"
