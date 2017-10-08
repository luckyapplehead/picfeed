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
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
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
train_set = spark.read.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_feature_test/part-*", schema=fieldSchema)
train_set_r = train_set.rdd.map(lambda p: Row(label=p.label, features=Vectors.dense(p.ctr, p.pnum, p.pdef, p.pbeau, p.s_term_score, p.sumclick, p.sumshow)))
print train_set_r.take(5)

print "finish map input"
train_set_d = spark.createDataFrame(train_set_r)
(training, test) = train_set_d.randomSplit([0.9, 0.1])
#train
lr = LogisticRegression(maxIter=10, regParam=0.3)
lrModel = lr.fit(training)
print "coefficients"
print lrModel.coefficients
print "intercept"
print lrModel.intercept
#summary
# $example on$
# Extract the summary from the returned LogisticRegressionModel instance trained
# in the earlier example
trainingSummary = lrModel.summary
# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)
# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

fprecision = trainingSummary.precisionByThreshold
print "fprecision"
fprecision.show()

maxPrecision = fprecision.groupBy().max('precision').select('max(precision)').head()['max(precision)']
print "max precision"
print maxPrecision
bestThreshold = fprecision.where(maxPrecision - fprecision.precision < 0.00001).select('threshold').head()
print "bestThreshold"
print bestThreshold


# Set the model threshold to maximize F-Measure
fMeasure = trainingSummary.fMeasureByThreshold
print "fMeasure"
fMeasure.show()

# set best threshold
maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()['max(F-Measure)']
print "maxFMeasure"
print maxFMeasure
fMeasure_new = spark.createDataFrame(fMeasure.rdd, ['threshold','fmeasure'])
bestThreshold = fMeasure_new.where(maxFMeasure - fMeasure_new.fmeasure < 0.00001).select('threshold').head()
print "bestThreshold"
print bestThreshold
print "original threshold"
print lr.getThreshold()


#lr.setThreshold(bestThreshold['threshold'])
lr.setThreshold(-3)
print "new threshold"
print lr.getThreshold()

#lr.setThreshold(bestThreshold)
# $example off$
#test
result_all = lrModel.transform(training)
evaluator = BinaryClassificationEvaluator(rawPredictionCol="prediction")
res = evaluator.evaluate(result_all)
print "evaluator res:"
print res
res = evaluator.evaluate(result_all, {evaluator.metricName: "areaUnderPR"})
print "evaluator pr res:"
print res

print "all result"
result_all.show()

result_all.createOrReplaceTempView("result_all")
sql_query = """
SELECT *
FROM result_all
WHERE label == 1
"""
result_all_1 = spark.sql(sql_query)
result_all_1.show()
result_one = result_all_1.head()
print "features"
print result_one.features
print "label"
print result_one.label
print "rawPrediction"
print result_one.rawPrediction
print "probability"
print result_one.probability
print "prediction"
print result_one.prediction


sql_query = """
SELECT *
FROM result_all
WHERE label == 1 and prediction > 0
"""
result_all_2 = spark.sql(sql_query)
result_all_2.show()

model_path = "gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/lr_model_test"
#lrModel.save(model_path)
print "finish"

