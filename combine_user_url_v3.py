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
sc = SparkContext('local')
spark = SparkSession(sc)
bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")
todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")

accum = sc.accumulator(0)

print "begin to map input"

train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_combine").map(lambda row: row.split("\t")).map(lambda p: Row(uid=p[0], urlid=p[1], ts=p[2], label=p[3]))
combine_uinfo = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files_combine_toterm_new/part-00000").map(lambda row: row.split("\t", 1))

print "finish to map input"

def process_uinfo(line):
    if len(line) != 2:
        return Row(urlid=line, urlinfo="")
    return Row(urlid=line[0], urlinfo=line[1])


print "begin to map and collect uinfo"
combine_uinfo_dict = combine_uinfo.map(process_uinfo)

print "finish map"

train_set_d = spark.createDataFrame(train_set)
combine_uinfo_dict_d = spark.createDataFrame(combine_uinfo_dict)

train_set_d.createOrReplaceTempView("train_set")
combine_uinfo_dict_d.createOrReplaceTempView("combine_uinfo_dict")

#combine_uinfo_dict_d.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/combine_uinfo_dict_tmp")

print "start join"
sql_query = """
SELECT train_set.*,combine_uinfo_dict.urlinfo
FROM train_set
LEFT OUTER JOIN combine_uinfo_dict
ON train_set.urlid = combine_uinfo_dict.urlid
"""
train_set_url = spark.sql(sql_query)
print "finish join"

accum.add(1)


print "accum:"

print accum

print train_set_url.take(2)

#train_set_url.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2")

train_set_url.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2")

print "finish save"

