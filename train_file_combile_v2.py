#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")
todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
# load train_set
input_directory1 = "gs://{}/tmp/train_set_raw-{}".format(bucket, todays_date)

print "tmpinput1:"
print input_directory1

conf1 = {
    # Input Parameters
    "mapred.bq.project.id": project,
    "mapred.bq.gcs.bucket": bucket,
    "mapred.bq.temp.gcs.path": input_directory1,
    "mapred.bq.input.project.id": project,
    "mapred.bq.input.dataset.id": "picfeed",
    "mapred.bq.input.table.id": "train_set",
    "spark.sql.shuffle.partitions": "1000",
    "spark.default.parallelism": "1000",
}

print "start load data"

# Read the data from BigQuery into Spark as an RDD.
table_data_train_set = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf1)

print "finish load data"

def process_uinfo(line):

    line = (line.uid[2:], line.urlid[2:], line.ts, line.label)
    return line

print "start map"
out_rdd = table_data_train_set.map(process_uinfo)

print "finish map"

print out_rdd.take(2)
out_rdd.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/train_set_v2")

print "finish save"

input_path1 = sc._jvm.org.apache.hadoop.fs.Path(input_directory1)
input_path1.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path1, True)

