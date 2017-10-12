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
from pyspark.ml.classification import GBTClassifier
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
train_set_r = train_set.rdd.map(lambda p: Row(label=p.label, features=Vectors.dense(p.ctr, p.pnum, p.pdef, p.pbeau,
 p.s_term_score, p.sumclick, p.sumshow)))
print train_set_r.take(5)
print "finish map input"
train_set_d = spark.createDataFrame(train_set_r)
(training, test) = train_set_d.randomSplit([0.99, 0.01])
#train
dt = GBTClassifier(subsamplingRate=0.7)
model = dt.fit(training)
print "model.totalNumNodes"
print model.totalNumNodes
print "model.treeWeights"

rint model.treeWeights
print "model.featureImportances"
print model.featureImportances
print "model.numClasses"
print model.numClasses
print(model.toDebugString)
print "model.trees"
model.trees
model_path = "gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/gbt_model"
model.save(model_path)
#predict
result_all = model.transform(test)
result_all.show(50)
result_all.select("label","prediction", "probability", "rawPrediction","features").show(5)
#evaluate
evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(result_all)
print("Test Error = %g " % (1.0 - accuracy))
evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction")
accuracy = evaluator.evaluate(result_all)
print("Test Error = %g " % (1.0 - accuracy))
result_all.createOrReplaceTempView("result_all")
sql_query = """
SELECT *
FROM result_all
WHERE label == 1
"""
result_all_1 = spark.sql(sql_query)
result_all_1.show()
sql_query = """
SELECT *
FROM result_all
WHERE label == 1 and prediction > 0
"""
result_all_2 = spark.sql(sql_query)
result_all_2.show()
print "finish"

