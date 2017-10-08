#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import sys
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

text_file = sc.textFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/data_combine_uinfo")
def process_uinfo(line):
    line = line.strip()
    line_arr = line.split('\t')
    for i in range(0, len(line_arr)):
        line_arr[i] = line_arr[i].strip()
        if len(line_arr[i]) == 0:
            line_arr[i] = "-"
    return "\t".join(line_arr)


#spark.setConf("spark.sql.shuffle.partitions", "1000")
#spark.setConf("spark.default.parallelism", "1000")

bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")

todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")

# load train_set
input_directory1 = "gs://{}/tmp/train_set-{}".format(bucket, todays_date)
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

# Read the data from BigQuery into Spark as an RDD.
table_data_train_set = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf1)

# load combine info
input_directory2 = "gs://{}/tmp/data_combine_uinfo-{}".format(bucket, todays_date)
print "tmpinput2:"
print input_directory2

conf2 = {
    # Input Parameters
    "mapred.bq.project.id": project,
    "mapred.bq.gcs.bucket": bucket,
    "mapred.bq.temp.gcs.path": input_directory2,
    "mapred.bq.input.project.id": project,
    "mapred.bq.input.dataset.id": "picfeed",
    "mapred.bq.input.table.id": "data_set_term",
    "spark.sql.shuffle.partitions": "1000",
    "spark.default.parallelism": "1000",
}

# Read the data from BigQuery into Spark as an RDD.
table_data_data_combine_uinfo = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf2)

table_data_train_set.createOrReplaceTempView("train_set")
table_data_data_combine_uinfo.createOrReplaceTempView("data_combine_uinfo")

#user_url_combine_data = table_data_train_set.join(table_data_data_combine_uinfo,  \
#	table_data_train_set.urlid == table_data_data_combine_uinfo.urlid, 'left_outer').select( \
#	table_data_train_set.uid, table_data_train_set.ts, table_data_train_set.label, table_data_data_combine_uinfo.'*').collect()

#user_url_combine_data = table_data_train_set.join(table_data_data_combine_uinfo, \
#        table_data_train_set.urlid == table_data_data_combine_uinfo.urlid, 'left_outer').collect()

print "start join"

sql_query = """
SELECT train_set.uid,train_set.ts,train_set.label,data_combine_uinfo.* 
FROM train_set 
LEFT OUTER JOIN data_combine_uinfo 
ON train_set.urlid = data_combine_uinfo.urlid
"""
user_url_combine_data = spark.sql(sql_query)

print "finish join"

print user_url_combine_data.take(2)
user_url_combine_data.saveAsTextFile("gs://dataproc-0e3e0110-db09-4037-98cc-dc355651aba0-asia-southeast1/tensorflow/data/picfeed/user_url_combine_data")

print "finish save"

input_path1 = sc._jvm.org.apache.hadoop.fs.Path(input_directory1)
input_path1.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path1, True)
input_path2 = sc._jvm.org.apache.hadoop.fs.Path(input_directory2)
input_path2.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path2, True)

