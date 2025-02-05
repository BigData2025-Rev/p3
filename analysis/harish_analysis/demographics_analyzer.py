from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
'''
What are the trends in racial composition in CA, NY, TX over the 30 years?

What are the trends in racial composition nationally over the 30 years?

What will be the overall racial composition of the USA in 2030?

'''

spark = SparkSession.builder.appName("HarishDataAnalysis").config("spark.master", "local[*]").getOrCreate()

# Path to the cleaned and finalized dataset used for the analysis.
path = "hdfs://localhost:9000/user/test/clean_data.orc"

# Read the ORC file and save it into a Spark DataFrame.
data = spark.read.orc(path)

# Summarizes the population by demographic for each census year in California, New York, and Texas.
states = data.where((data.state_abbr == "CA") & (data.summary_level == 40) | (data.state_abbr == "TX") & (data.summary_level == 40) | (data.state_abbr == "NY") & (data.summary_level == 40))
states = states.groupBy("state_abbr", "year", "summary_level")
states = states.sum("white_population", "black_population", "american_indian_population", "asian_population", "native_hawaiian_population", "other_race_population", "two_or_more_races_population")
states = states.orderBy("state_abbr", "year")
states = states.select(col("state_abbr").alias("State"), 
                 col("year").alias("Year"), 
                 col("sum(white_population)").alias("White"), 
                 col("sum(black_population)").alias("Black"), 
                 col("sum(american_indian_population)").alias("American Indian"), 
                 col("sum(asian_population)").alias("Asian"), 
                 col("sum(native_hawaiian_population)").alias("Native Hawaiian"), 
                 col("sum(other_race_population)").alias("Other Race"), 
                 col("sum(two_or_more_races_population)").alias("Two Or More Races"))
states.show()

# Summarizes the population by demographic for each census year nationally.
national = data.where(data.summary_level == 40)
national = national.groupBy("year")
national = national.sum("white_population", "black_population", "american_indian_population", "asian_population", "native_hawaiian_population", "other_race_population", "two_or_more_races_population")
national = national.orderBy("year")
national = national.select(col("year").alias("Year"), 
                 col("sum(white_population)").alias("White"), 
                 col("sum(black_population)").alias("Black"), 
                 col("sum(american_indian_population)").alias("American Indian"), 
                 col("sum(asian_population)").alias("Asian"), 
                 col("sum(native_hawaiian_population)").alias("Native Hawaiian"), 
                 col("sum(other_race_population)").alias("Other Race"), 
                 col("sum(two_or_more_races_population)").alias("Two Or More Races"))

# Linear regression algorithm to predict future population.
def linear_regression(df, column, prediction_year):

    n = df.select("Year").count()
    x_sum = df.select(sum("Year")).collect()[0][0]
    y_sum = df.select(sum(column)).collect()[0][0]
    temp = df.withColumn("x_squared_sum", df.Year * df.Year)
    x_squared_sum = temp.select(sum("x_squared_sum")).collect()[0][0]
    temp = temp.withColumn("xy_sum", temp.Year * temp[column])
    xy_sum = temp.select(sum("xy_sum")).collect()[0][0]

    intercept = (y_sum*x_squared_sum - x_sum * xy_sum) / (n * x_squared_sum - (x_sum ** 2))
    slope = (n * xy_sum - x_sum * y_sum) / (n * x_squared_sum - (x_sum ** 2))
    prediction = intercept + slope * prediction_year

    return round(prediction)

# Predicts the US population by demographic for the 2030 census year.
prediction_2030 = spark.createDataFrame([(2030, 
                                          linear_regression(national, "White", 2030), 
                                          linear_regression(national, "Black", 2030), 
                                          linear_regression(national, "American Indian", 2030), 
                                          linear_regression(national, "Asian", 2030), 
                                          linear_regression(national, "Native Hawaiian", 2030), 
                                          linear_regression(national, "Other Race", 2030), 
                                          linear_regression(national, "Two Or More Races", 2030))])
national = national.union(prediction_2030)
national.show()

# Writes the DataFrames to CSVs.
states.coalesce(1).write.option("header", True).format("csv").mode("overwrite").save("hdfs://localhost:9000/user/test/1_P3_states")
national.coalesce(1).write.option("header", True).format("csv").mode("overwrite").save("hdfs://localhost:9000/user/test/1_P3_national")

spark.stop()
