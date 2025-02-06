import os
from pyspark.sql import SparkSession
from pyspark.sql import functions


CURRENT_DIR = os.getcwd()
INPUT_FILE = "file://" + os.path.join(CURRENT_DIR, 'clean_data_2025.orc')


spark = SparkSession.builder.appName("district-data-analysis").getOrCreate()
df = spark.read.orc(INPUT_FILE)
# Create view for Queries that are specific to each year.
table = df.createOrReplaceTempView("census")

# Create a CSV to track the average population of top 15 districts across 3 census years.
df = df.filter(df['summary_level']==500)
df = df.withColumn("district", functions.col("district").cast("int"))
df = df.groupBy("state_abbr", "district", "name").agg(
                            functions.avg("total_population").alias("Average Population"),
                            functions.count("district").alias("Count")
                            )
df = df.filter(df['Count'] == 3)
df = df.orderBy("Average Population", ascending=False).limit(10)
df.write.mode("overwrite").option("header", "true").csv("file://" + os.path.join(os.getcwd(), "district_population_average"))

# Generate district data for each year.
result_2000 = spark.sql("SELECT state_abbr, year, name, district, MAX(total_population) tp FROM census WHERE summary_level=500 AND year=2000 GROUP BY state_abbr, name, district, year ORDER BY tp DESC LIMIT 15")
result_2010 = spark.sql("SELECT state_abbr, year, name, district, MAX(total_population) tp FROM census WHERE summary_level=500 AND year=2010 GROUP BY state_abbr, name, district, year ORDER BY tp DESC LIMIT 15")
result_2020 = spark.sql("SELECT state_abbr, year, name, district, MAX(total_population) tp FROM census WHERE summary_level=500 AND year=2020 GROUP BY state_abbr, name, district, year ORDER BY tp DESC LIMIT 15")

union_df = result_2000.union(result_2010).union(result_2020)
union_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("file://" + os.path.join(os.getcwd(), "result"))

spark.stop()

