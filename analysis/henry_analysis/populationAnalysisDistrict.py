'''
Regional Comparison:
    Which districts experienced the highest population growth and decline rates between census years?
	SUMLEV 500, Year, State, Abbr, Name, total_population, growth rate calculation
	Bar/column chart
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, avg
from pyspark.sql.functions import concat_ws, lit

spark = SparkSession.builder.appName("Population Growth Analysis").getOrCreate()

# path to cleaned data used for analysis
path = "file:///Users/henrylee/Projects-Revature/Project3_USCensusData/clean_data.orc"

# read ORC file and save into Spark Dataframe
df = spark.read.orc(path)

'''How do population growth rates in metropolitan districts compare to non-metropolitan districts?'''
df_filtered = df.filter(col("summary_level") == 500) #filter for SUMLEV 500
#df_filtered.show()
df_POP2000 = df_filtered.filter(col("year") == 2000).select("state_abbr", "name", "district", "total_population").withColumnRenamed("total_population", "total_population_2000")
#df_POP2000.show()
df_POP2010 = df_filtered.filter(col("year") == 2010).select("state_abbr", "name", "district", "total_population").withColumnRenamed("total_population", "total_population_2010")
#df_POP2010.show()
df_POP2020 = df_filtered.filter(col("year") == 2020).select("state_abbr", "name", "district", "total_population").withColumnRenamed("total_population", "total_population_2020")
#df_POP2020.show()

## convert district column to integer because the data type for year 2000 is float, but string in year 2010 and 2020 
df_POP2000_mod = df_POP2000.withColumn(
   "district_1",  col("district").cast('int')
)
#df_POP2000_mod.show()

df_POP2010_mod = df_POP2010.withColumn(
   "district_1",  col("district").cast('int')
)
#df_POP2010_mod.show()

df_POP2020_mod = df_POP2020.withColumn(
   "district_1",  col("district").cast('int')
)
#df_POP2020_mod.show()

df_POP2010_mod = df_POP2010_mod.drop('district')
df_POP2020_mod = df_POP2020_mod.drop('district')
combined_pop_df = df_POP2000_mod.join(df_POP2010_mod, ["state_abbr", "name", "district_1"], 'full')
combined_pop_df = combined_pop_df.join(df_POP2020_mod, ["state_abbr", "name", "district_1"], "full")
#combined_pop_df.show()

combined_df = df_filtered.join(combined_pop_df, ["state_abbr", "name", "district"])
#combined_df.show()

# calculate population growth between decades 2000s and 2010s
df_popGrowthMet1 = combined_df.withColumn(
    "2000-2010_Growth_Rate",
    when(col("total_population_2000") > 0, (((col("total_population_2010")) - col("total_population_2000")) / col("total_population_2000")) * 100).otherwise(None)
)

#df_popGrowthMet1.show()

df_popGrowthMet2 = combined_df.withColumn(
    "2010-2020_Growth_Rate",
    when(col("total_population_2010") > 0, (((col("total_population_2020")) - col("total_population_2010")) / col("total_population_2010")) * 100).otherwise(None)
)

#df_popGrowthMet2.show()

# create a new dataframe with selected columns from the existing dataframe
df_popGrowthMet2_new = df_popGrowthMet2.select("state_abbr", "name", "district", "2010-2020_Growth_Rate")
#df_popGrowthMet2_new.show()

combined_grow_df = df_popGrowthMet1.join(df_popGrowthMet2_new, ["state_abbr", "name", "district"], 'full')
#combined_grow_df.show()

#combined_df.show()


# write to CSV
output_file_loc = "file:///Users/henrylee/Projects-Revature/Project3_USCensusData/"
output_file = output_file_loc + "district_pop_growth.csv"
combined_grow_df.write.mode("overwrite").csv(output_file, header=True)

spark.stop()

