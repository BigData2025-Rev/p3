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

#Question

path = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/Data2020/allStates/orc/clean_data.orc"
output = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/regions.csv"

df = spark.read.orc(path)
df.createOrReplaceTempView("census")

result = spark.sql('SELECT year, SUM(total_population), region FROM census WHERE summary_level < 50 GROUP BY region, year SORT BY region, year')
result.show(200, False)
result.coalesce(1).write.csv(output, header=True, mode="overwrite")


#path = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/Data2020/allStates/orc/2010_combine.orc"
#path20 = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/Data2020/allStates/orc/2020_combined_states.orc"

#path = "file:///mnt/c/Users/patri/Desktop/RevatureTraining/Project3/Data2020/combined_each_state/National/2020_combined_national.orc"

#result = spark.sql('SELECT year, ANY_VALUE(state_abbr), SUM(total_population) FROM census WHERE state_abbr = "AZ" GROUP BY year;')
#result = spark.sql('SELECT year, state_abbr, name, total_population, region FROM census WHERE summary_level = 20 SORT BY region')
#result = spark.sql('Select STUSAB, SUMLEV, REGION, NAME FROM census WHERE SUMLEV < 50')

# resultNE = spark.sql('SELECT year, SUM(total_population) as population, region FROM census WHERE summary_level = 40 AND region = "Northeast" GROUP BY region, year SORT BY year')
# resultMW = spark.sql('SELECT year, SUM(total_population) as population, region FROM census WHERE summary_level = 40 AND region = "Midwest" GROUP BY region, year SORT BY year')
# resultS = spark.sql('SELECT year, SUM(total_population) as population, region FROM census WHERE summary_level = 40 AND region = "South" GROUP BY region, year SORT BY year')
# resultW = spark.sql('SELECT year, SUM(total_population) as population, region FROM census WHERE summary_level = 40 AND region = "West" GROUP BY region, year SORT BY year')
# plt.plot(resultNE.toPandas()["year"], resultNE.toPandas()["population"], label = "Northeast")
# plt.plot(resultMW["year"], resultMW["population"], label = "Midwest")
# plt.plot(resultS["year"], resultS["population"], label = "South")
# plt.plot(resultW["year"], resultW["population"], label = "West")
# plt.legend()
# plt.show()

#df10 = spark.read.orc(path10)
#df20 = spark.read.orc(path20)


#df.show(1000, False)
#print("Rows:" + str(df.count()))