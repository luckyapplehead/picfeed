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
print "begin to map input"
train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2/part-00000-eab41fe7-4a1c-46e5-b995-6beba43de164-c000.csv").map(lambda row: row.split(",", 4)).map(lambda p: Row(uid=p[0], urlid=p[1], ts=p[2], label=p[3], urlinfo=p[4]))
print "finish to map input"
print train_set.take(2)

train_set_d = spark.createDataFrame(train_set)

train_set_d.createOrReplaceTempView("train_set")

print "start select"
sql_query = """
SELECT train_set.uid, train_set.urlinfo
FROM train_set
WHERE train_set.label > 0
"""
train_set_urlinfo = spark.sql(sql_query)
print train_set_urlinfo.take(2)

def process_uinfo(line):
    p=line.urlinfo.split("\t")
    r = Row(uid=line.uid, utype=p[0], url=p[1], title=p[2], s_title=p[3],  \
    pnum=p[4], pdef=p[5], pbeau=p[6], ppoint=p[7], tpoint=p[8], s_content=p[9])

    return r

train_set_s_uinfo = train_set_urlinfo.rdd.map(process_uinfo)
print "finish split urlinfo"
print train_set_s_uinfo.take(2)

train_set_s_uinfo_d = spark.createDataFrame(train_set_s_uinfo)
train_set_s_title = train_set_s_uinfo_d.select("uid", "s_title")
print "finish select urlinfo"
print train_set_s_title.head(2)

def combine_uinfo(a, b):
    term_dict = dict()
    s_title = a
    if s_title != "-":
        arr = s_title.split(",")
        for i in range(0, len(arr)):
            (k,v) = arr[i].split(":")
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)
    s_title = b
    if s_title != "-":
        arr = s_title.split(",")
        for i in range(0, len(arr)):
            (k,v) = arr[i].split(":")
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)
    s = ""
    for (k,v) in term_dict.items():
        if len(s) > 0:
            s += ","
        s = s + k + ":" + str(v)
    return s

print "start reduce by key"
s_title_combine = train_set_s_title.rdd.reduceByKey(combine_uinfo)

print s_title_combine.take(2)

print "finish"
