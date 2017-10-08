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
train_set = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data_v2/part-00000-eab41fe7-4a1c-46e5-b995-6beba43de164-c000.csv").map( \
    lambda row: row.split(",", 4)).map(lambda p: Row(label=p[0], ts=p[1], uid=p[2], urlid=p[3], urlinfo=p[4]))

def process_train_set(line):
    toparr=line.urlinfo.split("\t")
    if len(toparr) != 10:
        r = Row(uid=line.uid, s_term="") 
        return r
    (utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content)=toparr

    term_dict = dict()
    if s_title != "-":
        arr = s_title.split(",")
        for i in range(0, len(arr)):
            (k,v) = arr[i].split(":")
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)

    if ppoint != "-":
        arr = ppoint.split(";")
        for i in range(0, len(arr)):
            (k,v) = arr[i].split(":")
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)

    if tpoint != "-":
        arr = tpoint.split(",")
        for i in range(0, len(arr)):
            k = arr[i]
            if term_dict.has_key(k):
                term_dict[k] += 1
            else:
                term_dict[k] = 1

    s = ""
    for (k,v) in term_dict.items():
        if len(s) > 0:
            s += ","
        if line.label != 0:
            s = s + k + ":" + str(v)
        else:
            s = s + k + ":" + str(-1 * v)


    r = Row(uid=line.uid, s_term=s)

    return r

train_set_s_term = train_set.map(process_train_set)

print "finish to map input"
print train_set_s_term.take(2)

def combine_uinfo(a, b):
    term_dict = dict()
    if a != "-" and a != "":
        arr = a.split(",")
        for i in range(0, len(arr)):
            subarr = arr[i].split(":")
            if len(subarr) != 2:
               continue
            (k,v) = subarr
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)

    if b != "-" and b != "":
        arr = b.split(",")
        for i in range(0, len(arr)):
            subarr = arr[i].split(":")
            if len(subarr) != 2:
                continue
            (k,v) = subarr
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
s_term_combine = train_set_s_term.reduceByKey(combine_uinfo)
print s_term_combine.take(2)
print "finish"


