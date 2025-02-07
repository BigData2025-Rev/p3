'''
Population trends and growth (metropolitan_pop_growth.csv): 
    How do population growth rates in metropolitan districts compare to non-metropolitan districts?
    SUMLEV 160, Metro Status, POP2000, POP2020, Growth Rate Calculation
    Box Plot, Histogram
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, avg

spark = SparkSession.builder.appName("Population Growth Analysis").getOrCreate()

# path to cleaned data used for analysis
#path = "hdfs://localhost:9000/user/hadoop/orc_data/clean_data.orc"
path = "file:///Users/henrylee/Projects-Revature/Project3_USCensusData/clean_data.orc"

# read ORC file and save into Spark Dataframe
df = spark.read.orc(path)

'''How do population growth rates in metropolitan districts compare to non-metropolitan districts?'''
df_filtered = df.filter(col("summary_level") == 160) #filter for SUMLEV 160
#df_filtered.show()
df_POP2000 = df_filtered.filter(col("year") == 2000).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2000")
#df_POP2000.show()
df_POP2020 = df_filtered.filter(col("year") == 2020).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2020")
#df_POP2020.show()

combined_pop_df = df_POP2000.join(df_POP2020, ["state_abbr", "name"])
#combined_pop_df.show()
combined_df = df_filtered.join(combined_pop_df, ["state_abbr", "name"])
#combined_df.show()

# calculate population growth between decades 2000s and 2020s
df_popGrowthMet = combined_df.withColumn(
    "Growth_Rate",
    when(col("total_population_2000") > 0, (((col("total_population_2020")) - col("total_population_2000")) / col("total_population_2000")) * 100).otherwise(None)
)

#df_popGrowthMet.show()

# write to CSV
output_file_loc = "file:///Users/henrylee/Projects-Revature/Project3_USCensusData/"
output_file = output_file_loc + "metropolitan_pop_growth.csv"
df_popGrowthMet.write.mode("overwrite").csv(output_file, header=True)

spark.stop()

