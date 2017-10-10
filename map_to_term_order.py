#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import jieba
from jieba import analyse

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
text_file = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files/*")
def process_uinfo(line):
    line = line.strip()
    line_arr = line.split('\t')
    if len(line_arr) != 10:
        return line
    (uid, utype, url, title, pnum, pdef, pbeau, ppoint, tpoint, tcontent) = line_arr
    l_title = jieba.cut(title, cut_all=False)
    s_title = ",".join(l_title)

    l_content = jieba.cut(tcontent, cut_all=False)
    s_content = ",".join(l_content)

    print "\t".join([uid, title, s_title, s_content])


out_rdd = text_file.map(process_uinfo)
print out_rdd.take(2)
out_rdd.coalesce(1).saveAsTextFile("gs://dataproc-1228d533-ffe2-4747-a056-8cd396c3db5f-asia-southeast1/data/picfeed/data_term_order")



