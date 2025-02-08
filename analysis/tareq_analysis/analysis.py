from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, udf, when, lit
from pyspark.sql.functions import lit
from pyspark.sql.types import FloatType

# Create Spark Session
spark = SparkSession.builder.appName("Analysis")\
        .config("spark.master", "local[*]") \
        .getOrCreate()

sc = spark.sparkContext

# Read ORC Data
df = spark.read.orc("file:///mnt/c/Users/ttaar/project3/p3/analysis/tareq_analysis/clean_data.orc")

# Filter for Summary Level 40
df = df.where("summary_level == 40")

# Select Relevant Columns
state_population = df.select("name", "total_population", "year")
state_population = state_population.withColumnRenamed("name", "state")

# Convert to Dictionary (for Broadcasting)
state_pop_dict = state_population.rdd.map(lambda row: ((row["state"], row["year"]), row["total_population"])).collectAsMap()

# Broadcast the Dictionary
broadcast_state_pop = sc.broadcast(state_pop_dict)

# Define the UDF
def calculate_growth_rate(state, fromYear, toYear):
    pop_from = broadcast_state_pop.value.get((state, fromYear), None)
    pop_to = broadcast_state_pop.value.get((state, toYear), None)

    if pop_from and pop_to and pop_from > 0:
        return round(((pop_to - pop_from) / pop_from) * 100, 2)
    return None  # Handle missing values

# Register UDF
growth_rate_udf = udf(calculate_growth_rate, FloatType())

# Apply UDF to Compute Growth Rate
growth_rate_2000_2010 = state_population.withColumn(
    "growth_rate_2000_2010",
    when(col("year") == 2000, growth_rate_udf(col("state"), lit(2000), lit(2010)))
)

growth_rate_2010_2020 = state_population.withColumn(
    "growth_rate_2010_2020",
    when(col("year") == 2000, growth_rate_udf(col("state"), lit(2010), lit(2020)))
)

growth_rate_2000_2020 = state_population.withColumn(
    "growth_rate_2000_2020",
    when(col("year") == 2000, growth_rate_udf(col("state"), lit(2000), lit(2020)))
)



growth_rate_2000_2010 = growth_rate_2000_2010.select("state", "growth_rate_2000_2010")\
    .where("growth_rate_2000_2010 is not NULL")
growth_rate_2010_2020 = growth_rate_2010_2020.select("state", "growth_rate_2010_2020")\
    .where("growth_rate_2010_2020 is not NULL")
growth_rate_2000_2020 = growth_rate_2000_2020.select("state", "growth_rate_2000_2020")\
    .where("growth_rate_2000_2020 is not NULL")

# Show Results

state_population.show(n=state_population.count(), truncate=False)
growth_rate_2000_2010.show(n=growth_rate_2000_2010.count(), truncate=False)
growth_rate_2010_2020.show(n=growth_rate_2010_2020.count(), truncate=False)
growth_rate_2000_2020.show(n=growth_rate_2000_2020.count(), truncate=False)

state_population.write.mode("overwrite").csv("file:///mnt/c/Users/ttaar/project3/p3/analysis/tareq_analysis/popOutput", header=True)
growth_rate_2000_2010.write.mode("overwrite").csv("file:///mnt/c/Users/ttaar/project3/p3/analysis/tareq_analysis/growthOutput/2000_2010", header=True)
growth_rate_2010_2020.write.mode("overwrite").csv("file:///mnt/c/Users/ttaar/project3/p3/analysis/tareq_analysis/growthOutput/2010_2020", header=True)
growth_rate_2000_2020.write.mode("overwrite").csv("file:///mnt/c/Users/ttaar/project3/p3/analysis/tareq_analysis/growthOutput/2000_2020", header=True)



