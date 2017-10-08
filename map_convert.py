#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
text_file = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_feature/part-*")

out_rdd = text_file.map(lambda x:x)
out_rdd.coalesce(1).saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_feature_compose")

