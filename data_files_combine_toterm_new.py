#!/usr/bin/python
# -*- coding: utf-8 -*-
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sc = SparkContext('local')
spark = SparkSession(sc)
text_file = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files_combine_toterm/part-00000")
def process_uinfo(line):
    line = line.strip()
    line_arr = line.split('\t')
    if len(line_arr) != 11:
    	return line
    (urlid, utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content) = line_arr
    urlid = urlid[2:]
    return "\t".join([urlid, utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content])
out_rdd = text_file.map(process_uinfo)
print out_rdd.take(2)
out_rdd.coalesce(1).saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_files_combine_toterm_new")

