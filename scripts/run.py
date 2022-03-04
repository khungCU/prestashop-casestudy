
### We can place the jar in /usr/local/spark-3.2.1-bin-hadoop3.2/jars/
### Or define the jar when setting the builder like this
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

import datetime
from utils import explode_nested_type


TABLE_LANGUAGE = os.getenv("TABLE_LANGUAGE")
TABLE_COMMIT = os.getenv("TABLE_COMMIT")
LANGUAGE = os.getenv("LANGUAGE")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
PARENT_PROJECT = os.getenv("PARENT_PROJECT")

spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('spark-read-from-bigquery') \
                    .config('parentProject', 'khung-playground') \
                    .config("credentialsFile", f"../{GOOGLE_API_KEY}").getOrCreate()
                    
####################         
# Language Dataset #
####################
duplicate_data = ["repo_name", "name"]

df_language = spark.read.format('bigquery').option('table', TABLE_LANGUAGE).load()
df_language = explode_nested_type(df_language)
df_language = explode_nested_type(df_language, "struct")
df_language = df_language.where(df_language["name"] == LANGUAGE)
df_language = df_language.dropDuplicates(duplicate_data)


##################
# Commit Dataset #
##################
commit_columns = ['commit', 'committer', 'repo_name']
df_commit = spark.read.format('bigquery').option('table', TABLE_COMMIT).load()
df_commit = df_commit.select(*commit_columns) # select useful columns
df_commit = explode_nested_type(df_commit, 'struct')
# rename column to prevent duplicate column name after join
df_commit = df_commit.withColumnRenamed("name","commiter_name")

# I aware there are records of commits are at exactally same time and same commiter, In my asscept this doesn't make sense so I see this as duplicate hence drop it.  
duplicate_data = ["commiter_name", "email", "time_sec", "tz_offset", "date", "repo_name"]
df_commit = df_commit.dropDuplicates(duplicate_data)


###################################
# Join df_language with df_commit #
###################################
df_commit_language = df_commit.join(df_language, on = ["repo_name"])


############################################################################################
# Window function on the merged data inorder to calculate time distribution of two commits #
############################################################################################
partition_columns = ["repo_name"]
windowSpec  = Window.partitionBy(partition_columns).orderBy("date")

df_commit_language = df_commit_language.withColumn("commit_seq",F.row_number().over(windowSpec))
df_commit_language = df_commit_language.withColumn("lag_time_sec", F.lag("time_sec", 1).over(windowSpec))
df_commit_language = df_commit_language.withColumn("prv_time_diff", df_commit_language["time_sec"]  - df_commit_language["lag_time_sec"])


############################
# Write back to filesystem #
############################
write_file_date = str(datetime.datetime.today().date())
file_name = f"{LANGUAGE}_commits_{write_file_date}.parguet"
df_commit_language.write \
                  .mode("overwrite") \
                  .parquet(f"../spark_output/{file_name}")
                  
                  
print("Job Done")