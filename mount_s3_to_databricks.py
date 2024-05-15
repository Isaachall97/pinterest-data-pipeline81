# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# COMMAND ----------

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0affee876ba9-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/user-0affee876ba9"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

#Check whether bucket is mounted properly
display(dbutils.fs.ls("/mnt/user-0affee876ba9"))


# COMMAND ----------

# MAGIC %sql
# MAGIC --Disable format checks during the reading of Delta Tables--
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# File location and type
file_location_geo = "/mnt/user-0affee876ba9/topics/0affee876ba9.geo/partition=0/*.json"
file_type = "json"
#Define the schema for this data
geo_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("country", StringType(), True)
])
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.schema(geo_schema) \
.load(file_location_geo)
# Display Spark dataframe to check its content
display(geo_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# File location and type
file_location_pin = "/mnt/user-0affee876ba9/topics/0affee876ba9.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
pin_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", IntegerType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])
# Read in JSONs from mounted S3 bucket
pin_df = spark.read.format(file_type) \
.schema(pin_schema) \
.load(file_location_pin)
# Display Spark dataframe to check its content
display(pin_df)


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# File location and type
file_location_user = "/mnt/user-0affee876ba9/topics/0affee876ba9.user/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
user_schema = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", StringType(), True)
])
# Read in JSONs from mounted S3 bucket
user_df = spark.read.format(file_type) \
.schema(user_schema) \
.load(file_location_user)
# Display Spark dataframe to check its content
display(user_df)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_geo = "/mnt/user-0affee876ba9/topics/0affee876ba9.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_geo)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_pin = "/mnt/user-0affee876ba9/topics/0affee876ba9.pin/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_pin)
# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location_user = "/mnt/user-0affee876ba9/topics/0affee876ba9.user/partition=0/*.json"
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location_user)
# Display Spark dataframe to check its content
display(df_user)

# COMMAND ----------

dbutils.fs.unmount("/mnt/user-0affee876ba9")
