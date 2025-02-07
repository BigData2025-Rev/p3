from pyspark.sql import SparkSession
from pyspark.sql.functions import count, regexp_replace, col, when, round

# Initialize Spark session
spark = SparkSession.builder.appName("CensusAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

final_orc_path = r"/mnt/c/Users/adeel/Desktop/final_orc/clean_data.orc"
output_path = r"/mnt/c/Users/adeel/Desktop/test45"

# Load ORC file
df = spark.read.orc(final_orc_path)

# filter by 500 (district-level)
df_filtered = df.filter(df["summary_level"] == 500)

# count for districts that appear in all 3 census years
df_counts = df_filtered.groupBy("state_abbr", "name").agg(count("year").alias("year_count"))

df_valid_districts = df_counts.filter(df_counts["year_count"] == 3).select("state_abbr", "name")

# join back to original df (keeping valid districts)
df = df_filtered.join(df_valid_districts, on=["state_abbr", "name"], how="inner")

# Remove leading 0 and .0 from 'district; col
df = df.withColumn("district", regexp_replace(col("district"), r"\.0$", ""))  # Remove ".0"
df = df.withColumn("district", regexp_replace(col("district"), r"^0+", ""))   # Remove leading zeros

# will have some empty districts now "at-large" bc they're just 0 and previous expression removes them, put the 0 back to empty district cols
df = df.withColumn(
    "district", 
    when(col("district") == "", 0).otherwise(col("district"))
)

# add total_youth_population col
df = df.withColumn("total_youth_population", col("total_population") - col("total_adult_pop"))

# Get population for 2000, 2010, and 2020 per district
df_2000 = df.filter(col("year") == 2000).select(
    "state_abbr", "district", "total_adult_pop", "total_youth_population"
).withColumnRenamed("total_adult_pop", "adult_2000") \
 .withColumnRenamed("total_youth_population", "youth_2000")

df_2010 = df.filter(col("year") == 2010).select(
    "state_abbr", "district", "total_adult_pop", "total_youth_population"
).withColumnRenamed("total_adult_pop", "adult_2010") \
 .withColumnRenamed("total_youth_population", "youth_2010")

df_2020 = df.filter(col("year") == 2020).select(
    "state_abbr", "district", "total_adult_pop", "total_youth_population"
).withColumnRenamed("total_adult_pop", "adult_2020") \
 .withColumnRenamed("total_youth_population", "youth_2020")

# Join 2000, 2010, and 2020 data to calculate growth rates
df_growth = df_2000.join(df_2010, on=["state_abbr", "district"], how="inner") \
                   .join(df_2020, on=["state_abbr", "district"], how="inner")

# Calculate Growth Rates
df_growth = df_growth.withColumn("adult_growth_2000_2010", round(((col("adult_2010") - col("adult_2000")) / col("adult_2000")) * 100, 2))
df_growth = df_growth.withColumn("adult_growth_2010_2020", round(((col("adult_2020") - col("adult_2010")) / col("adult_2010")) * 100, 2))
df_growth = df_growth.withColumn("adult_growth_2000_2020", round(((col("adult_2020") - col("adult_2000")) / col("adult_2000")) * 100, 2))

df_growth = df_growth.withColumn("youth_growth_2000_2010", round(((col("youth_2010") - col("youth_2000")) / col("youth_2000")) * 100, 2))
df_growth = df_growth.withColumn("youth_growth_2010_2020", round(((col("youth_2020") - col("youth_2010")) / col("youth_2010")) * 100, 2))
df_growth = df_growth.withColumn("youth_growth_2000_2020", round(((col("youth_2020") - col("youth_2000")) / col("youth_2000")) * 100, 2))

# Select only required columns
df_final = df_growth.select(
    "state_abbr", "district", 
    "adult_2000", "adult_2010", "adult_2020", 
    "adult_growth_2000_2010", "adult_growth_2010_2020", "adult_growth_2000_2020",
    "youth_2000", "youth_2010", "youth_2020", 
    "youth_growth_2000_2010", "youth_growth_2010_2020", "youth_growth_2000_2020"
)

# Save to CSV
df_final.coalesce(1).write.mode("overwrite").csv(output_path, header=True)