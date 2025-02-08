from pyspark.sql import SparkSession
from pyspark.sql.functions import count, regexp_replace, col, when

# Initialize Spark session
spark = SparkSession.builder.appName("CensusAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

final_orc_path = r"/mnt/c/Users/adeel/Desktop/final_orc/clean_data.orc"
output_path = r"/mnt/c/Users/adeel/Desktop/test3"

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

# Select only the required columns
df = df.select(
    col("year"),
    col("name"),
    col("state_abbr"),
    col("district"),
    col("total_population"),
    col("total_adult_pop"),
    col("total_youth_population")
)

df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)