import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, abs

spark = SparkSession.builder\
    .appName("ReadCleanedDataset")\
    .master("local[*]")\
    .config("spark.hadoop.fs.defaultFS", "file:///")\
    .getOrCreate()

df_origional = spark.read.orc("/content/clean_data.orc")
df_origional.printSchema()
df_origional.coalesce(1).write.mode("overwrite").option("header", "true").csv("/content/clean_data.csv")

"""
  Question: What percentage of districts experienced population stagnation (less than 2% change)?
  Process;
  1. First get all districts (sumlev=500) and its population for each year from origional table
  2. Join those tables with state and distince name.
  3. Generated three new columns for each district:
    - pop_chg_00_10: population change from 2000 to 2010, in percentage
    - pop_chg_10_20: population change from 2010 to 2020, in percentage
    - pop_chg_overall: population change from 2000 to 2020, in percentage
  4. Generate pie chart and calculate percentage of districts experienced population stagnation (less than 2% change) in powerbi
"""

# take population data for each decade for each district
df = df_origional.filter(col("summary_level")==500)
df_2000 = df.filter(col("year")==2000).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2000")
df_2010 = df.filter(col("year")==2010).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2010")
df_2020 = df.filter(col("year")==2020).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2020")

# join them all as a single table and then calculate population change rate
df_joined = df_2000.join(df_2010, ["state_abbr", "name"], "inner").join(df_2020, ["state_abbr", "name"], "inner")
df_joined = df_joined.withColumn("distinct_id", concat(col("state_abbr"), lit("-"),col("name")))
df_joined = df_joined.withColumn("pop_chg_00_10", (col("total_population_2010")-col("total_population_2000"))/(col("total_population_2000"))*100)
df_joined = df_joined.withColumn("pop_chg_10_20", (col("total_population_2020")-col("total_population_2010"))/(col("total_population_2010"))*100)
df_joined = df_joined.withColumn("pop_chg_overall", (col("total_population_2020")-col("total_population_2000"))/(col("total_population_2000"))*100)

# determine if the district experienced population stagnation (less than 2% change) for each decade and overall
df_joined = df_joined.withColumn("pop_stgn_00_10", when(abs(col("pop_chg_00_10")) < 2, lit("True")).otherwise(lit("False")))
df_joined = df_joined.withColumn("pop_stgn_10_20", when(abs(col("pop_chg_10_20")) < 2, lit("True")).otherwise(lit("False")))
df_joined = df_joined.withColumn("pop_stgn_overall", when(abs(col("pop_chg_overall")) < 2, lit("True")).otherwise(lit("False")))

df_joined.write.mode("overwrite").option("header", "true").csv("/content/question1/")

spark.stop()