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
from pyspark.ml.feature import ChiSqSelector
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
sc = SparkContext('local')
spark = SparkSession(sc)
fieldSchema = StructType([StructField("ctr", DoubleType(), True),
  StructField("label", IntegerType(), True),
  StructField("pdef", DoubleType(), True),
  StructField("pbeau", DoubleType(), True),
  StructField("pnum", IntegerType(), True),
  StructField("s_term_score", DoubleType(), True),
  StructField("sumclick", LongType(), True),
  StructField("sumshow", LongType(), True),
  StructField("uid", LongType(), True)
])


print "begin to map input"
train_set = spark.read.csv("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/train_feature_compose_new/part-*", schema=fieldSchema)
train_set_d = train_set.rdd.map(lambda p: Row(label=p.label, features=Vectors.dense(p.ctr, p.pnum, p.pdef, p.pbeau, p.s_term_score, p.sumclick, p.sumshow)))
print train_set_d.take(5)
print "finish map input"
df = spark.createDataFrame(train_set_d, ['features', 'label'])
df.show()

selector = ChiSqSelector(numTopFeatures=3, featuresCol="features",
                             outputCol="selectedFeatures", labelCol="label")

#result = selector.fit(df).transform(df)
model = selector.fit(df)
result = model.transform(df)

print("ChiSqSelector output with top %d features selected" % selector.getNumTopFeatures())
result.show()
print result.head().selectedFeatures

selector_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/chi-sq-selector"
selector.save(selector_path)

model_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/chi-sq-selector-model"
model.save(model_path)
print "finish"

