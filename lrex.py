#!/usr/bin/python
"""Run a linear regression using Apache Spark ML.

In the following PySpark (Spark Python API) code, we take the following actions:

  * Load a previously created linear regression (Google BigQuery) input table
    into our Google Cloud Dataproc Spark cluster as an RDD (Resilient
    Distributed Dataset)
  * Transform the RDD into a Spark Dataframe
  * Vectorize the features on which the model will be trained
  * Compute a linear regression using Spark ML

"""
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from datetime import datetime
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
# The imports, above, allow us to access SparkML features specific to linear
# regression as well as the Vectors types.


# Define a function that collects the features of interest
# (mother_age, father_age, and gestation_weeks) into a vector.
# Package the vector in a tuple containing the label (`weight_pounds`) for that
# row.


def vector_from_inputs(r):
  return (int(r["label"]), Vectors.dense(float(r["ts_interval"]),
                                            float(r["p_num"]),
                                            float(r["p_defination"]),
                                            float(r["p_beauty"]),
                                            float(r["title_term_score"]),
                                            float(r["url_whole_term_score"])))

# Use Cloud Dataprocs automatically propagated configurations to get
# the Google Cloud Storage bucket and Google Cloud Platform project for this
# cluster.
bucket = spark._jsc.hadoopConfiguration().get("fs.gs.system.bucket")
project = spark._jsc.hadoopConfiguration().get("fs.gs.project.id")

# Set an input directory for reading data from Bigquery.
todays_date = datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%S")
input_directory = "gs://{}/tmp/picfeed-{}".format(bucket, todays_date)

# Set the configuration for importing data from BigQuery.
# Specifically, make sure to set the project ID and bucket for Cloud Dataproc,
# and the project ID, dataset, and table names for BigQuery.

conf = {
    # Input Parameters
    "mapred.bq.project.id": project,
    "mapred.bq.gcs.bucket": bucket,
    "mapred.bq.temp.gcs.path": input_directory,
    "mapred.bq.input.project.id": project,
    "mapred.bq.input.dataset.id": "picfeed",
    "mapred.bq.input.table.id": "train4",
}

# Read the data from BigQuery into Spark as an RDD.
table_data = spark.sparkContext.newAPIHadoopRDD(
    "com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "com.google.gson.JsonObject",
    conf=conf)

# Extract the JSON strings from the RDD.
table_json = table_data.map(lambda x: x[1])

# Load the JSON strings as a Spark Dataframe.
picfeed_data = spark.read.json(table_json)
# Create a view so that Spark SQL queries can be run against the data.
picfeed_data.createOrReplaceTempView("picfeed_train4")


# As a precaution, run a query in Spark SQL to ensure no NULL values exist.
sql_query = """
SELECT *
from picfeed_train4
where userid is not null
and ts_interval is not null
and p_num is not null
and p_defination is not null
and p_beauty is not null
and title_term_score is not null
and url_whole_term_score is not null
"""
clean_data = spark.sql(sql_query)

# Create an input DataFrame for Spark ML using the above function.
training_data = clean_data.rdd.map(vector_from_inputs).toDF(["label",
                                                             "features"])
training_data.cache()

# Construct a new LinearRegression object and fit the training data.
lr = LogisticRegression(maxIter=500, regParam=0.2, elasticNetParam=0.8)
model = lr.fit(training_data)
# Print the model summary.
print "Coefficients:" + str(model.coefficients)
print "Intercept:" + str(model.intercept)


from pyspark.ml.classification import LogisticRegression

trainingSummary = model.summary

objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)

trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

fMeasure = trainingSummary.fMeasureByThreshold
maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
#bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']).select('threshold').head()['threshold']
#lr.setThreshold(bestThreshold)


