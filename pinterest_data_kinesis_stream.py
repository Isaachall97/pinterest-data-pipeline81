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

# MAGIC %sql
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affee876ba9-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()


df_pin = df_pin.selectExpr("CAST(data as STRING)")
display(df_pin)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

pin_schema = StructType([
    StructField("index", IntegerType()),
    StructField("unique_id", StringType()),
    StructField("title", StringType()),
    StructField("description", StringType()),
    StructField("poster_name", StringType()),
    StructField("follower_count", StringType()),
    StructField("tag_list", StringType()),
    StructField("is_image_or_video", StringType()),
    StructField("image_src", StringType()),
    StructField("downloaded", IntegerType()),
    StructField("save_location", StringType()),
    StructField("category", StringType())
])

# COMMAND ----------

from pyspark.sql.functions import from_json, col

# Use from_json() to parse the JSON data with the defined schema
df_parsed = df_pin.withColumn("parsed_data", from_json("data", pin_schema))

# Now, flatten the structure to have each JSON key as a column
pin_data = df_parsed.select(
    col("parsed_data.*")  # Expands the columns within the parsed_data struct
)

display(pin_data)

# COMMAND ----------

#Perform Data cleaning and transformation on the posts data
#Replace blank columns with None
df_pin_cleaned = pin_data.replace({"":None})
# Handle 'k', 'K', 'm', 'M' in follower_count
df_pin_cleaned = df_pin_cleaned.withColumn(
    "follower_count",
    when(
        col("follower_count").rlike("(?i)k$"),
        regexp_replace(col("follower_count"), "(?i)k$", "").cast("float") * 1000
    ).when(
        col("follower_count").rlike("(?i)m$"),
        regexp_replace(col("follower_count"), "(?i)m$", "").cast("float") * 1000000
    ).otherwise(col("follower_count"))
)
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
final_pin_df = df_pin_cleaned.select(columns_order)
display(final_pin_df)

# COMMAND ----------

#Writes post data delta table to the _checkpoints directory
final_pin_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affee876ba9_pin_table")

# COMMAND ----------

#Removes delta tables from the _checkpoints directory
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

# COMMAND ----------

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affee876ba9-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_geo = df_geo.selectExpr("CAST(data as STRING)")
display(df_geo)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

geo_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("country", StringType())
])


# COMMAND ----------

from pyspark.sql.functions import from_json, col

# Use from_json() to parse the JSON data with the defined schema
geo_parsed = df_geo.withColumn("parsed_data", from_json("data", geo_schema))

# Now, flatten the structure to have each JSON key as a column
geo_data = geo_parsed.select(
    col("parsed_data.*")  # Expands the columns within the parsed_data struct
)

display(geo_data)

# COMMAND ----------

#Perform data transformation and cleaning on the geolocation data
#Create a new column 'coordinates' that is an array of the 'latitude' and 'longitude' column
df_geo_cleaned = geo_data.withColumn("coordinates", array(col("latitude"), col("longitude")))
#Drop the latitude and longitude columns
df_geo_cleaned = df_geo_cleaned.drop("latitude")
df_geo_cleaned = df_geo_cleaned.drop("longitude")
#Convert timestamp data from string type to timestamp type
df_geo_cleaned = df_geo_cleaned.withColumn("timestamp", to_timestamp("timestamp"))
#Order the columns
column_order_geo = ['ind', 'country', 'coordinates', 'timestamp']
final_geo_df = df_geo_cleaned.select(column_order_geo)
display(final_geo_df)

# COMMAND ----------

#Writes delta table for geolocation to the _checkpoints directory
final_geo_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affee876ba9_geo_table")

# COMMAND ----------

#Removes delta tables from the _checkpoints directory
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)

# COMMAND ----------

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0affee876ba9-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

df_user = df_user.selectExpr("CAST(data as STRING)")
display(df_user)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

user_schema = StructType([
    StructField("ind", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", IntegerType()),
    StructField("date_joined", DateType())
])

# COMMAND ----------

from pyspark.sql.functions import from_json, col

# Use from_json() to parse the JSON data with the defined schema
user_parsed = df_user.withColumn("parsed_data", from_json("data", user_schema))

# Now, flatten the structure to have each JSON key as a column
user_data = user_parsed.select(
    col("parsed_data.*")  # Expands the columns within the parsed_data struct
)

display(user_data)

# COMMAND ----------

#Clean and transform user data
#Combine the first_name and last_name column into new column 'user_name' 
df_user_cleaned = user_data.withColumn("user_name", concat("first_name", "last_name"))
#Drop the first_name and last_name columns
df_user_cleaned = df_user_cleaned.drop("first_name")
df_user_cleaned = df_user_cleaned.drop("last_name")
#Order data frame
user_column_order = ['ind', 'user_name', 'age', 'date_joined']
final_user_df = df_user_cleaned.select(user_column_order)
display(final_user_df)

# COMMAND ----------

#Writes the user data in a delta table to the _checkpoints directory
final_user_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0affee876ba9_user_table")

# COMMAND ----------

#Removes delta tables from the _checkpoints directory
dbutils.fs.rm("/tmp/kinesis/_checkpoints/", True)
