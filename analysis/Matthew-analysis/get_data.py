from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag
spark = SparkSession.builder.getOrCreate()

df = spark.read.orc("clean_data.orc")
#Filtering to just county data
df = df.filter(df.summary_level == 50).select('year',"state_abbr",'name',"total_population", "total_adult_pop")
df = df.withColumn("youth_percentage", (1 - col("total_adult_pop") / col("total_population"))*100)
#Using a window function to get the difference in youth percentage from previous census
dfwindow = Window.partitionBy("state_abbr","name").orderBy("year")
df = df.withColumn("change_in_percentage", col("youth_percentage") - lag("youth_percentage").over(dfwindow))
df.coalesce(1).write.csv("adult-pop-data.csv",header = True)