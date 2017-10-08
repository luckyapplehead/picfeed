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

train_set_join_urlid_label_raw = spark.read.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_join_urlid_label/part-*")
train_set_join_urlid_label_raw.head()
print "finish train_set_join_urlid_label_raw"

train_set_join_urlid_label = spark.createDataFrame(train_set_join_urlid_label_raw.rdd, ['forcount', 'label', 'ts', 'uid', 'urlid', 'urlinfo', 'sumclick', 'sumshow'])

#train_set_join_urlid_label = train_set_join_urlid_label_raw.rdd.map( \
#    lambda p: Row(label=int(p[1]), ts=p[2], uid=int(p[3]), urlid=int(p[4]), urlinfo=p[5][1:][:-1], sumclick=int(p[6]), sumshow=int(p[7])))


train_set_join_urlid_label.take(5)
print "finish train_set_join_urlid_label"


def process_train_set(line):
    if line.urlinfo is None:
        r = Row(uid=line.uid, urlid=line.urlid, label=line.label, ts=line.ts, sumclick=line.sumclick, sumshow=line.sumshow, s_term="-", pnum=-1, pdef=-1, pbeau=-1)
        return r
    if len(line.urlinfo) < 3:
        r = Row(uid=line.uid, urlid=line.urlid, label=line.label, ts=line.ts, sumclick=line.sumclick, sumshow=line.sumshow, s_term="-", pnum=-1, pdef=-1, pbeau=-1)
        return r
    urlinfo = line.urlinfo[1:][:-1]
    toparr=urlinfo.split("\t")
    if len(toparr) != 10:
        r = Row(uid=line.uid, urlid=line.urlid, label=line.label, ts=line.ts, sumclick=line.sumclick, sumshow=line.sumshow, s_term="-", pnum=-1, pdef=-1, pbeau=-1)
        return r
    (utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content)=toparr
    term_dict = dict()
    if s_title != "-":
        arr = s_title.split(",")
        for i in range(0, len(arr)):
            subarr = arr[i].split(":")
            if len(subarr) != 2:
               continue
            (k,v) = subarr
            if term_dict.has_key(k):
                term_dict[k] += float(v)
            else:
                term_dict[k] = float(v)
    if ppoint != "-":
        arr = ppoint.split(";")
        for i in range(0, len(arr)):
            subarr = arr[i].split(":")
            if len(subarr) != 2:
               continue
            (k,v) = subarr
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
    # append term info
    for (k,v) in term_dict.items():
        if len(s) > 0:
            s += ","
        if line.label != 0:
            s = s + k + ":" + str(v)
        else:
            s = s + k + ":" + str(-1 * v)

    r = Row(uid=line.uid, urlid=line.urlid, label=line.label, ts=line.ts, sumclick=line.sumclick, sumshow=line.sumshow, s_term=s, pnum=pnum, pdef=pdef, pbeau=pbeau)

    return r


train_set_s_term = train_set_join_urlid_label.rdd.map(process_train_set)
print train_set_s_term.take(5)
print "finish process_train_set"

train_set_s_term_d = spark.createDataFrame(train_set_s_term)

train_set_s_term_d.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_s_term")

train_set_s_term_d.createOrReplaceTempView("train_set_s_term")

sql_query = """
SELECT uid, s_term
FROM train_set_s_term
WHERE uid is not null
"""
uid_s_term = spark.sql(sql_query)
print uid_s_term.head()
print "finish to sql"


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
uid_s_term_combine = uid_s_term.rdd.reduceByKey(combine_uinfo)
#uid_s_term_combine.cache()
print uid_s_term_combine.take(5)

# join each sample with usermodel
uid_s_term_combine_d = spark.createDataFrame(uid_s_term_combine, ['uid', 'user_s_term'])

uid_s_term_combine_d.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/uid_s_term_combine")

uid_s_term_combine_d.createOrReplaceTempView("uid_s_term_combine")
print "finish reduce by key"


print "start join"
join_sql_query = """
SELECT train_set_s_term.*, uid_s_term_combine.user_s_term
FROM train_set_s_term
LEFT OUTER JOIN uid_s_term_combine
ON train_set_s_term.uid = uid_s_term_combine.uid
"""
train_set_join_user_model = spark.sql(join_sql_query)
print train_set_join_user_model.head(5)
train_set_join_user_model.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_join_user_model")
print "finish join"

def construct_feature(line):
    #r = Row(uid=line.uid, urlid=line.urlid, label=line.label, ts=line.ts, sumclick=line.sumclick, sumshow=line.sumshow, s_term=s, pnum=pnum, pdef=pdef, pbeau=pbeau)
    s_term_score = 0
    if line.user_s_term == "" or line.s_term:
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
                s_term_score += float(v) * user_term_dict[k]
    ctr = float(line.sumclick) / float(line.sumshow)
    r = Row(uid=line.uid, label=line.label, sumclick=line.sumclick, sumshow=line.sumshow, ctr=ctr, pnum=int(line.pnum), pdef=float(line.pdef), pbeau=float(line.pbeau), s_term_score=s_term_score)
    return r

train_feature = train_set_join_user_model.rdd.map(construct_feature)
print train_feature.take(10)

train_feature_d = spark.createDataFrame(train_feature)

train_feature_d.write.csv("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_feature")
print "finish contruct feature"

print "finish"



