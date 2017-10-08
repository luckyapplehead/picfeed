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
def process_uinfo(filename):
    file = open(filename)
    while 1:
        line = file.readline()
        if not line:
            break
        line = line.strip()
        line_arr = line.split('\t')
        print line
        if len(line_arr) != 10:
            continue
        (uid, utype, url, title, pnum, pdef, pbeau, ppoint, tpoint, tcontent) = line_arr
        l_title = jieba.analyse.extract_tags(title, topK=20, withWeight=True)
        s_title = ""
        for i in range(0, len(l_title)):
            if i > 0:
                s_title = s_title + ","
            s_title = s_title + l_title[i][0] + ":" + str(l_title[i][1])

        l_content = jieba.analyse.extract_tags(tcontent, topK=40, withWeight=True)
        s_content = ""
        for i in range(0, len(l_content)):
            if i > 0:
                s_content = s_content + ","
            s_content = s_content + l_content[i][0] + ":" + str(l_content[i][1])
        print "\t".join([uid, utype, url, title, s_title, pnum, pdef, pbeau, ppoint, tpoint, s_content])
    file.close()

if __name__=='__main__':
    process_uinfo(sys.argv[1])

