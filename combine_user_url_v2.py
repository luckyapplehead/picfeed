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

train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_combine").map(lambda row: row.split("\t"))
combine_uinfo = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files_combine_toterm_new/part-00000").map(lambda row: row.split("\t", 1))

print "finish to map input"

def process_uinfo(line):
    if len(line) != 2:
        return Row(urlid=line, urlinfo="")
    return Row(urlid=line[0], urlinfo=line[1])

#combine_uinfo_dict = combine_uinfo.map(lambda p: Row(urlid=p[0], urlinfo=p[1])).collect()

print "begin to map and collect uinfo"
combine_uinfo_dict = combine_uinfo.map(process_uinfo).collect()

print "finish map"

combine_uinfo_b = sc.broadcast(combine_uinfo_dict)

print "finish broadcast"

def update(line, uinfo):
    line = (line, uinfo.filter(uinfo.urlid==line[1]).urlinfo)
    return line

print "begin update"
train_set_url = train_set.map(lambda x: update(x, combine_uinfo_b))

print "finish update"

accum.add(1)

print "finish mapping"

print "accum:"

print accum

print train_set_url.take(2)

train_set_url.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2")
print "finish save"


