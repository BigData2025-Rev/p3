# Jarrod  What regions will have the highest population in 2030?
# 	SUMLEV 40, Region, GroupBy(Year, Region)
# 	Linear regression on regions, predict 2030 value. Either plot or display in Map.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, format_number



# Initialize Spark Session
spark = SparkSession.builder.appName("PopulationPredictionsByRegion").getOrCreate()


# Read From Hdfs
df = spark.read.orc("/user/jarrod/clean_data.orc")

# Filter and Aggregate Population by Region & Year
data = df.filter(col("summary_level") == 40).select("year", "region", "total_population").dropna()
region_df = data.groupBy("year", "region").agg(sum("total_population").alias("TotalPopulation"))

# Pivot to Get Population in Columns for Easier Growth Calculation
region_pivot = region_df.groupBy("region").pivot("year").sum("TotalPopulation")

# Calculate Growth Rates
region_pivot = region_pivot.withColumn("Growth_2000_2010", (col("2010") - col("2000")) / col("2000"))
region_pivot = region_pivot.withColumn("Growth_2010_2020", (col("2020") - col("2010")) / col("2010"))

# Average Growth Rate for Future Prediction
region_pivot = region_pivot.withColumn("Avg_Growth_Rate", (col("Growth_2000_2010") + col("Growth_2010_2020")) / 2)

# Predict 2030 Population Using Average Growth Rate
region_pivot = region_pivot.withColumn("2030_Prediction", col("2020") * (1 + col("Avg_Growth_Rate")))

# Format Output for Readability
region_pivot = region_pivot.select(
    "region", 
    format_number("2000", 0).alias("2000_Population"),
    format_number("2010", 0).alias("2010_Population"),
    format_number("2020", 0).alias("2020_Population"),
    format_number("2030_Prediction", 0).alias("2030_Predicted_Population"),
    format_number("Avg_Growth_Rate", 4).alias("Avg_Growth_Rate")
)

# Show Results
region_pivot.show()

spark.stop()
