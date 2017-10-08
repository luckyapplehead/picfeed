
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
from pyspark.sql.functions import array
sc = SparkContext('local')
spark = SparkSession(sc)
print "begin to map input"
fieldSchema = StructType([StructField("label", IntegerType(), True),
  StructField("pdef", DoubleType(), True),
  StructField("pbeau", DoubleType(), True),
  StructField("pnum", IntegerType(), True),
  StructField("s_term", StringType(), True),
  StructField("sumclick", LongType(), True),
  StructField("sumshow", LongType(), True),
  StructField("ts", LongType(), True),
  StructField("uid", LongType(), True),
  StructField("urlid", LongType(), True),
  StructField("user_s_term", StringType(), True)
])
train_set_join_user_model= spark.read.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_join_user_model/part-*", schema=fieldSchema)
print "finish read train_set_join_user_model"
train_set_join_user_model.createOrReplaceTempView("train_set_join_user_model")
train_set_join_user_model.head()

print "finish join"
def construct_feature(line):
    s_term_score = 0
    if (line.user_s_term is None) or (line.user_s_term == "") or (line.s_term is None) or (line.s_term==""):        
        s_term_score = 0
    else:
        #contruct user term dict
        user_term_dict = dict()
        user_term_arr = line.user_s_term.split(",")
        for i in range(0, len(user_term_arr)):
            subarr = user_term_arr[i].split(":")
            if len(subarr) != 2:
                continue
            (k,v) = subarr
            if user_term_dict.has_key(k):
                user_term_dict[k] += float(v)
            else:
                user_term_dict[k] = float(v)
        # cal s_term_score
        s_term_arr = line.s_term.split(",")
        for i in range(0, len(s_term_arr)):
            subarr = s_term_arr[i].split(":")
            if len(subarr) != 2:
                continue
            (k,v) = subarr
            if user_term_dict.has_key(k):
                s_term_score += float(v) * float(user_term_dict[k])
    ctr = float(line.sumclick) / float(line.sumshow)
    r = Row(uid=line.uid, label=line.label, sumclick=line.sumclick, sumshow=line.sumshow, ctr=ctr, pnum=int(line.pnum), pdef=float(line.pdef), pbeau=float(line.pbeau), s_term_score=float(s_term_score))
    return r
train_feature = train_set_join_user_model.rdd.map(construct_feature)
print train_feature.take(10)
train_feature_d = spark.createDataFrame(train_feature)
train_feature_d.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_feature")
print "finish contruct feature"
print "finish"

