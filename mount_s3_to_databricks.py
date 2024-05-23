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
final_geo_df = df_geo_cleaned.select(column_order_geo)
display(final_geo_df)

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

#Create temporary views so that SQL queries can be performed.
final_geo_df.createOrReplaceTempView("geo_data")
final_pin_df.createOrReplaceTempView("pin_data")
final_user_df.createOrReplaceTempView("user_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Query that finds the most popular category by country, and lists them along with the number of mentions for each category
# MAGIC */
# MAGIC
# MAGIC WITH CategoryCount AS (
# MAGIC   SELECT 
# MAGIC     gd.country, 
# MAGIC     pd.category, 
# MAGIC     COUNT(pd.category) AS category_count
# MAGIC   FROM
# MAGIC     geo_data gd
# MAGIC   JOIN 
# MAGIC     pin_data pd ON gd.ind = pd.ind
# MAGIC   GROUP BY 
# MAGIC     gd.country, 
# MAGIC     pd.category
# MAGIC ),
# MAGIC LargestCategory AS (
# MAGIC   SELECT 
# MAGIC     country, 
# MAGIC     category, 
# MAGIC     category_count,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY country ORDER BY category_count DESC) AS largest
# MAGIC   FROM 
# MAGIC     CategoryCount
# MAGIC )
# MAGIC SELECT 
# MAGIC   country, 
# MAGIC   category, 
# MAGIC   category_count
# MAGIC FROM 
# MAGIC   LargestCategory
# MAGIC WHERE 
# MAGIC   largest = 1
# MAGIC ORDER BY
# MAGIC   category_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Finds what the most popular category by year was
# MAGIC */
# MAGIC
# MAGIC WITH CategoryOverTime AS(
# MAGIC   SELECT 
# MAGIC     pd.category AS category,
# MAGIC     COUNT(pd.category) AS categories_number,
# MAGIC     YEAR(gd.timestamp) AS yeartime
# MAGIC   FROM 
# MAGIC     pin_data pd
# MAGIC   JOIN
# MAGIC     geo_data gd ON pd.ind = gd.ind
# MAGIC   WHERE 
# MAGIC     gd.timestamp BETWEEN '2018-01-01 00:00:00' AND '2021-12-31 23:59:59'
# MAGIC   GROUP BY
# MAGIC     pd.category,
# MAGIC     yeartime
# MAGIC   ORDER BY 
# MAGIC     categories_number,
# MAGIC     category
# MAGIC     )
# MAGIC   
# MAGIC   SELECT 
# MAGIC     SUM(categories_number) AS category_count, 
# MAGIC     yeartime AS post_year,
# MAGIC     category
# MAGIC   FROM
# MAGIC     CategoryOverTime
# MAGIC   GROUP BY
# MAGIC     post_year,
# MAGIC     category
# MAGIC   ORDER BY
# MAGIC     post_year;
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC
# MAGIC   
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Finds the country containing the user with the most followers
# MAGIC */
# MAGIC
# MAGIC WITH followCount AS (
# MAGIC   SELECT
# MAGIC     gd.country,
# MAGIC     pd.poster_name,
# MAGIC     pd.follower_count
# MAGIC   FROM 
# MAGIC     pin_data pd
# MAGIC   INNER JOIN 
# MAGIC     geo_data gd ON pd.ind = gd.ind
# MAGIC   GROUP BY
# MAGIC     gd.country,
# MAGIC     pd.poster_name,
# MAGIC     pd.follower_count),
# MAGIC
# MAGIC
# MAGIC largestFollowingCountry AS(
# MAGIC   SELECT
# MAGIC     country, 
# MAGIC     poster_name,
# MAGIC     follower_count,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY country ORDER BY follower_count DESC) AS largest
# MAGIC   FROM
# MAGIC     followCount),
# MAGIC
# MAGIC largestFollowingUser AS (
# MAGIC   SELECT
# MAGIC   country,
# MAGIC   poster_name,
# MAGIC   follower_count,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY country ORDER BY follower_count DESC) AS largestFollower
# MAGIC FROM
# MAGIC   largestFollowingCountry
# MAGIC WHERE
# MAGIC   largest = 1
# MAGIC ORDER BY
# MAGIC   follower_count DESC)
# MAGIC
# MAGIC SELECT
# MAGIC   country,
# MAGIC   follower_count
# MAGIC FROM 
# MAGIC   largestFollowingUser
# MAGIC WHERE
# MAGIC   largestFollower = 1
# MAGIC LIMIT
# MAGIC   1;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Finds the most popular category among different age groups
# MAGIC */
# MAGIC
# MAGIC WITH ageGroups AS(
# MAGIC SELECT
# MAGIC   usr.age,
# MAGIC   pd.category,
# MAGIC   COUNT(pd.category) AS category_count,
# MAGIC   CASE
# MAGIC     WHEN age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC     WHEN age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC     WHEN age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC     WHEN age >50 THEN '50+'
# MAGIC   END AS age_group
# MAGIC FROM
# MAGIC   pin_data pd
# MAGIC JOIN
# MAGIC   user_data usr ON pd.ind = usr.ind
# MAGIC GROUP BY
# MAGIC   pd.category,
# MAGIC   usr.age),
# MAGIC
# MAGIC  ageCategories AS( 
# MAGIC   SELECT
# MAGIC     age,
# MAGIC     category,
# MAGIC     category_count,
# MAGIC     age_group,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY category_count DESC) AS largest
# MAGIC   FROM 
# MAGIC     ageGroups
# MAGIC   ORDER BY
# MAGIC     category_count DESC)
# MAGIC
# MAGIC SELECT 
# MAGIC   age_group,
# MAGIC   category,
# MAGIC   category_count
# MAGIC FROM 
# MAGIC   ageCategories
# MAGIC WHERE 
# MAGIC   largest = 1
# MAGIC ORDER BY
# MAGIC   category_count DESC;
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC /*Finds the median follower count for different age groups*/
# MAGIC
# MAGIC
# MAGIC WITH ageGroups AS (
# MAGIC   SELECT 
# MAGIC     ud.age, 
# MAGIC     pd.follower_count,
# MAGIC     CASE
# MAGIC       WHEN ud.age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC       WHEN ud.age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC       WHEN ud.age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC       WHEN ud.age > 50 THEN '50+'
# MAGIC     END AS age_group
# MAGIC   FROM 
# MAGIC     user_data ud
# MAGIC   JOIN 
# MAGIC     pin_data pd ON ud.ind = pd.ind
# MAGIC ),
# MAGIC rankedFollowers AS (
# MAGIC   SELECT
# MAGIC     age_group,
# MAGIC     follower_count,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY follower_count) AS row_num,
# MAGIC     COUNT(*) OVER (PARTITION BY age_group) AS total_rows
# MAGIC   FROM 
# MAGIC     ageGroups
# MAGIC ),
# MAGIC medianCalc AS (
# MAGIC   SELECT
# MAGIC     age_group,
# MAGIC     follower_count,
# MAGIC     row_num,
# MAGIC     total_rows,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE total_rows / 2
# MAGIC     END AS median_low,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE (total_rows / 2) + 1
# MAGIC     END AS median_high
# MAGIC   FROM 
# MAGIC     rankedFollowers
# MAGIC )
# MAGIC SELECT
# MAGIC   age_group,
# MAGIC   AVG(follower_count) AS median_follower_count
# MAGIC FROM
# MAGIC   medianCalc
# MAGIC WHERE
# MAGIC   row_num IN (median_low, median_high)
# MAGIC GROUP BY
# MAGIC   age_group
# MAGIC ORDER BY
# MAGIC   age_group;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Finds the number of users joined from the years 2015-2020 by year
# MAGIC */
# MAGIC
# MAGIC WITH yearJoined AS(
# MAGIC   SELECT DISTINCT
# MAGIC     COUNT(usr.user_name) AS number_users_joined,
# MAGIC     CASE
# MAGIC       WHEN usr.date_joined BETWEEN '2015-01-01 00:00:00' AND '2015-12-31 23:59:59' THEN '2015'
# MAGIC       WHEN usr.date_joined BETWEEN '2016-01-01 00:00:00' AND '2016-12-31 23:59:59' THEN '2016'
# MAGIC       WHEN usr.date_joined BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59' THEN '2017'
# MAGIC       WHEN usr.date_joined BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59' THEN '2018'
# MAGIC       WHEN usr.date_joined BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 23:59:59' THEN '2019'
# MAGIC       WHEN usr.date_joined BETWEEN '2020-01-01 00:00:00' AND '2020-12-31 23:59:59' THEN '2020'
# MAGIC     END AS post_year
# MAGIC   FROM
# MAGIC     user_data usr
# MAGIC   GROUP BY
# MAGIC     post_year
# MAGIC   ORDER BY
# MAGIC     number_users_joined DESC),
# MAGIC
# MAGIC yearCategories AS(
# MAGIC   SELECT
# MAGIC     number_users_joined, 
# MAGIC     post_year, 
# MAGIC     ROW_NUMBER() OVER (PARTITION BY number_users_joined ORDER BY post_year DESC) AS largest
# MAGIC   FROM 
# MAGIC     yearJoined
# MAGIC   ORDER BY
# MAGIC     post_year DESC)
# MAGIC
# MAGIC SELECT 
# MAGIC   number_users_joined, 
# MAGIC   post_year
# MAGIC FROM 
# MAGIC   yearCategories
# MAGIC WHERE
# MAGIC   largest = 1
# MAGIC ORDER BY
# MAGIC   post_year ASC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Finds the median follower count of users who joined by year between 2015 and 2020
# MAGIC */
# MAGIC
# MAGIC WITH yearJoined AS(
# MAGIC   SELECT DISTINCT
# MAGIC     usr.user_name AS users_joined,
# MAGIC     pd.follower_count AS followers,
# MAGIC     CASE
# MAGIC       WHEN usr.date_joined BETWEEN '2015-01-01 00:00:00' AND '2015-12-31 23:59:59' THEN '2015'
# MAGIC       WHEN usr.date_joined BETWEEN '2016-01-01 00:00:00' AND '2016-12-31 23:59:59' THEN '2016'
# MAGIC       WHEN usr.date_joined BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59' THEN '2017'
# MAGIC       WHEN usr.date_joined BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59' THEN '2018'
# MAGIC       WHEN usr.date_joined BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 23:59:59' THEN '2019'
# MAGIC       WHEN usr.date_joined BETWEEN '2020-01-01 00:00:00' AND '2020-12-31 23:59:59' THEN '2020'
# MAGIC     END AS post_year
# MAGIC   FROM
# MAGIC     user_data usr
# MAGIC   JOIN 
# MAGIC     pin_data pd ON usr.ind = pd.ind
# MAGIC   GROUP BY
# MAGIC     post_year,
# MAGIC     users_joined,
# MAGIC     followers
# MAGIC   ORDER BY
# MAGIC     post_year DESC),
# MAGIC
# MAGIC rankedFollowers AS (
# MAGIC   SELECT
# MAGIC     users_joined,
# MAGIC     followers,
# MAGIC     post_year,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY users_joined ORDER BY followers) AS row_num,
# MAGIC     COUNT(*) OVER (PARTITION BY users_joined) AS total_rows
# MAGIC   FROM 
# MAGIC     yearJoined),
# MAGIC
# MAGIC medianCalc AS (
# MAGIC   SELECT
# MAGIC     users_joined,
# MAGIC     followers,
# MAGIC     row_num,
# MAGIC     post_year,
# MAGIC     total_rows,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE total_rows / 2
# MAGIC     END AS median_low,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE (total_rows / 2) + 1
# MAGIC     END AS median_high
# MAGIC   FROM 
# MAGIC     rankedFollowers
# MAGIC )
# MAGIC SELECT
# MAGIC   post_year,
# MAGIC   COUNT(followers) AS median_follower_count
# MAGIC FROM
# MAGIC   medianCalc
# MAGIC WHERE
# MAGIC   row_num IN (median_low, median_high)
# MAGIC GROUP BY
# MAGIC   post_year
# MAGIC ORDER BY
# MAGIC   post_year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Finds the median follower count by year joined (2015-2020) of users belonging to different age groups*/
# MAGIC
# MAGIC WITH yearJoined AS(
# MAGIC   SELECT DISTINCT
# MAGIC     usr.age AS age,
# MAGIC     usr.user_name AS users_joined,
# MAGIC     pd.follower_count AS followers,
# MAGIC     CASE
# MAGIC       WHEN usr.date_joined BETWEEN '2015-01-01 00:00:00' AND '2015-12-31 23:59:59' THEN '2015'
# MAGIC       WHEN usr.date_joined BETWEEN '2016-01-01 00:00:00' AND '2016-12-31 23:59:59' THEN '2016'
# MAGIC       WHEN usr.date_joined BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59' THEN '2017'
# MAGIC       WHEN usr.date_joined BETWEEN '2018-01-01 00:00:00' AND '2018-12-31 23:59:59' THEN '2018'
# MAGIC       WHEN usr.date_joined BETWEEN '2019-01-01 00:00:00' AND '2019-12-31 23:59:59' THEN '2019'
# MAGIC       WHEN usr.date_joined BETWEEN '2020-01-01 00:00:00' AND '2020-12-31 23:59:59' THEN '2020'
# MAGIC     END AS post_year
# MAGIC   FROM
# MAGIC     user_data usr
# MAGIC   JOIN 
# MAGIC     pin_data pd ON usr.ind = pd.ind
# MAGIC   GROUP BY
# MAGIC     post_year,
# MAGIC     users_joined,
# MAGIC     followers,
# MAGIC     age
# MAGIC   ORDER BY
# MAGIC     post_year DESC),
# MAGIC
# MAGIC ageGroups AS (
# MAGIC   SELECT 
# MAGIC     age,
# MAGIC     users_joined, 
# MAGIC     followers,
# MAGIC     post_year,
# MAGIC     CASE
# MAGIC       WHEN age BETWEEN 18 AND 24 THEN '18-24'
# MAGIC       WHEN age BETWEEN 25 AND 35 THEN '25-35'
# MAGIC       WHEN age BETWEEN 36 AND 50 THEN '36-50'
# MAGIC       WHEN age > 50 THEN '50+'
# MAGIC     END AS age_group
# MAGIC   FROM 
# MAGIC     yearJoined
# MAGIC ),
# MAGIC
# MAGIC rankedFollowers AS (
# MAGIC   SELECT
# MAGIC     age_group,
# MAGIC     users_joined,
# MAGIC     followers,
# MAGIC     post_year,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY users_joined ORDER BY followers) AS row_num,
# MAGIC     COUNT(*) OVER (PARTITION BY users_joined) AS total_rows
# MAGIC   FROM 
# MAGIC     ageGroups),
# MAGIC
# MAGIC medianCalc AS (
# MAGIC   SELECT
# MAGIC     age_group,
# MAGIC     users_joined,
# MAGIC     followers,
# MAGIC     row_num,
# MAGIC     post_year,
# MAGIC     total_rows,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE total_rows / 2
# MAGIC     END AS median_low,
# MAGIC     CASE
# MAGIC       WHEN total_rows % 2 = 1 THEN (total_rows + 1) / 2
# MAGIC       ELSE (total_rows / 2) + 1
# MAGIC     END AS median_high
# MAGIC   FROM 
# MAGIC     rankedFollowers
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   COUNT(followers) AS median_follower_count,
# MAGIC   post_year,
# MAGIC   age_group
# MAGIC FROM
# MAGIC   medianCalc
# MAGIC WHERE
# MAGIC   row_num IN (median_low, median_high)
# MAGIC GROUP BY
# MAGIC   post_year,
# MAGIC   age_group
# MAGIC ORDER BY
# MAGIC   post_year,
# MAGIC   age_group DESC;
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/user-0affee876ba9")
