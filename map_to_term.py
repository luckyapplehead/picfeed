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
    l_title = analyse.extract_tags(title, topK=20, withWeight=True)
    s_title = ""
    for i in range(0, len(l_title)):
        if i > 0:
            s_title = s_title + ","
        s_title = s_title + l_title[i][0] + ":" + str(l_title[i][1])

    l_content = analyse.extract_tags(tcontent, topK=40, withWeight=True)
    s_content = ""
    for i in range(0, len(l_content)):
        if i > 0:
            s_content = s_content + ","
        s_content = s_content + l_content[i][0] + ":" + str(l_content[i][1])

    return "\t".join([uid, utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content])

out_rdd = text_file.map(process_uinfo)
print out_rdd.take(2)
out_rdd.coalesce(1).saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files_combine_toterm")



