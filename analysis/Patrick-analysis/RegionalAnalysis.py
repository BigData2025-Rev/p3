import matplotlib.pyplot
from pyspark.sql import SparkSession
import os
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("CombineStateCSVs") \
    .config("spark.master", "local[4]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.local.dir", "/tmp/spark-temp/") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

#Columns: |unique_key   |year|state_abbr|logrecno|summary_level|county|name|district|total_population|
# white_population|black_population|american_indian_population|asian_population|native_hawaiian_population|
# other_race_population|two_or_more_races_population|total_adult_pop|region|metro_status|

#Define file paths
path = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/Data2020/allStates/orc/clean_data.orc"
output = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/regions.csv"

#Create dataframe from orc file with the census data
df = spark.read.orc(path)

#Query the data for the total population for each region in each decade
df.createOrReplaceTempView("census")
result = spark.sql('SELECT year, SUM(total_population), region FROM census WHERE summary_level = 40 GROUP BY region, year SORT BY region, year')

#Show and write the results
result.show(200, False)
result.coalesce(1).write.csv(output, header=True, mode="overwrite")