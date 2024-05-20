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

#Perform data transformation and cleaning on the geolocation data
#Create a new column 'coordinates' that is an array of the 'latitude' and 'longitude' column
df_geo_cleaned = df_geo.withColumn("coordinates", array(col("latitude"), col("longitude")))
#Drop the latitude and longitude columns
df_geo_cleaned = df_geo_cleaned.drop("latitude")
df_geo_cleaned = df_geo_cleaned.drop("longitude")
#Convert timestamp data from string type to timestamp type
df_geo_cleaned = df_geo_cleaned.withColumn("timestamp", to_timestamp("timestamp"))
#Order the columns
column_order_geo = ['ind', 'country', 'coordinates', 'timestamp']
final_df_geo = df_geo_cleaned.select(column_order_geo)
display(final_df_geo)

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

#Perform Data cleaning and transformation on the posts data
#Replace blank columns with None
df_pin_cleaned = df_pin.replace({"":None})
#Replace all non-integer values in column 'follower_count' with empty string
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", regexp_replace(col("follower_count"), "[^0-9]", ""))
#Rows with non-numeric values will have an empty string after cleaning
df_pin_cleaned = df_pin_cleaned.filter(col("follower_count") != "")
#Convert the column 'follower_count' to type 'int'
df_pin_cleaned = df_pin_cleaned.withColumn("follower_count", col("follower_count").cast("int"))
#Convert column 'downloaded' from type long to type int
df_pin_cleaned = df_pin_cleaned.withColumn("downloaded", col("downloaded").cast("int"))
#Clean the save_location column so that it only contains the filepath preceded by empty string
df_pin_cleaned = df_pin_cleaned.withColumn("save_location", regexp_replace(col("save_location"), "^Local save in ", ""))
#Get rid of empty string at the beginning of 'save_location'
df_pin_cleaned = df_pin_cleaned.withColumn("save_location", regexp_replace(col("save_location"), "^\\s*", ""))
#Rename column 'index' to 'ind'
df_pin_cleaned = df_pin_cleaned.withColumnRenamed("index", "ind")
#Order columns in the desired order
columns_order = ['ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category']
final_df = df_pin_cleaned.select(columns_order)
display(final_df)




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

#Clean and transform user data
#Combine the first_name and last_name column into new column 'user_name' 
df_user_cleaned = df_user.withColumn("user_name", concat("first_name", "last_name"))
#Drop the first_name and last_name columns
df_user_cleaned = df_user_cleaned.drop("first_name")
df_user_cleaned = df_user_cleaned.drop("last_name")
#Order data frame
user_column_order = ['ind', 'user_name', 'age', 'date_joined']
final_user_df = df_user_cleaned.select(user_column_order)
display(final_user_df)

# COMMAND ----------

dbutils.fs.unmount("/mnt/user-0affee876ba9")
