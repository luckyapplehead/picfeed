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
from pyspark.ml.recommendation import ALSModel
sc = SparkContext('local')
spark = SparkSession(sc)
print "begin to map input"


train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2/part-*").map( \
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
(training, test) = ratings.randomSplit([0.99, 0.01])
#als = ALS(maxIter=10, regParam=0.01, numUserBlocks=20, numItemBlocks=100, userCol="uid", itemCol="urlid", ratingCol="label")
#model = als.fit(ratings)
#model.setColdStartStrategy("drop");

# Save and load model
als_path="gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/als_conf"
#als.save(als_path)
als = ALS.load(als_path)

model_path = "gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/als_model"
#model.save(model_path)
model = ALSModel.load(model_path)


user_recs = model.recommendForAllUsers(10)
print "user recs"
print user_recs.head()
user_recs.rdd.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_recs")
item_recs = model.recommendForAllItems(10)
print "item recs"
print item_recs.head()
item_recs.rdd.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/item_recs")


# print user feature
userfeature = model.userFactors.orderBy("id")
print "User feature"
print userfeature.head(5)
#userfeature.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/userfeature")


# Evaluate the model by computing the RMSE on the test data
print "start to predict"
predictions = model.transform(test)
evaluator = RegressionEvaluator(metricName="rmse", labelCol="label",
                                predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))
print predictions.head(5)
#predictions.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/predictions")
print "finish predictions"

print "finish"


