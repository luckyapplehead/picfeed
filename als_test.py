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
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
sc = SparkContext('local')
spark = SparkSession(sc)
print "begin to map input"
train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2/part-00000-eab41fe7-4a1c-46e5-b995-6beba43de164-c000.csv").map( \
    lambda row: row.split(",", 4)).map(lambda p: Row(label=int(p[0]), ts=p[1], uid=int(p[2]), urlid=int(p[3]), urlinfo=p[4]))

print train_set.take(5)
print "finish map input"

# get url show click

train_set_d = spark.createDataFrame(train_set)

train_set_d.createOrReplaceTempView("train_set")

sql_query = """
SELECT uid, urlid, label
FROM train_set
"""
ratings = spark.sql(sql_query)
print ratings.head()
print "finish to sql uid_urlid_label"

(training, test) = ratings.randomSplit([0.8, 0.2])

als = ALS(maxIter=5, regParam=0.01, userCol="uid", itemCol="urlid", ratingCol="label")
model = als.fit(training)

# Evaluate the model by computing the RMSE on the test data
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="label",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

predictions.head(5)
predictions.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/predictions_test")
print "finish predictions"

# Save and load model
als_path="gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/als_model_test"
als.save(als_path)
als2 = ALS.load(als_path)
# $example off$
print "finish load"


