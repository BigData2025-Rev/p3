from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, format_number

# Initialize Spark Session
spark = SparkSession.builder.appName("PopulationPredictionsByRegion").getOrCreate()

# Read Data
df = spark.read.orc("/user/jarrod/clean_data.orc")

# Filter and Aggregate Population by Region & Year
data = df.filter(col("summary_level") == 40).select("year", "region", "total_population").dropna()
region_df = data.groupBy("year", "region").agg(sum("total_population").alias("TotalPopulation"))

# Pivot to Get Population in Columns for Growth Rate Calculation
region_pivot = region_df.groupBy("region").pivot("year").sum("TotalPopulation")

# Calculate Growth Rates for Past Decades
region_pivot = region_pivot.withColumn("Growth_2000_2010", (col("2010") - col("2000")) / col("2000"))
region_pivot = region_pivot.withColumn("Growth_2010_2020", (col("2020") - col("2010")) / col("2010"))

# Average Growth Rate
region_pivot = region_pivot.withColumn("Avg_Growth_Rate", (col("Growth_2000_2010") + col("Growth_2010_2020")) / 2)

# Predict 2030 Population Using Growth Rate
region_pivot = region_pivot.withColumn("Growth_Prediction_2030", col("2020") * (1 + col("Avg_Growth_Rate")))

# -------------------------------------------
# 🔹 PART 2: Apply Linear Regression Model
# -------------------------------------------

from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegression

# Index Region for Model
indexer = StringIndexer(inputCol="region", outputCol="regionIndex")
region_df = indexer.fit(region_df).transform(region_df)

# Assemble Features
assembler = VectorAssembler(inputCols=["year", "regionIndex"], outputCol="features")
region_df = assembler.transform(region_df)

# Train Linear Regression Model
lr = LinearRegression(featuresCol="features", labelCol="TotalPopulation", regParam=0.1)
model = lr.fit(region_df)

# Predict 2030 Using Linear Regression
regions = region_df.select("region").distinct()
future_data = regions.withColumn("year", lit(2030))
future_data = indexer.fit(future_data).transform(future_data)
future_data = assembler.transform(future_data)

predictions = model.transform(future_data)

# -------------------------------------------
# 🔹 PART 3: Merge Growth & Regression Predictions
# -------------------------------------------

# Get 2020 Population for Reference
df2020 = region_df.filter(col("year") == 2020).select("region", "TotalPopulation")

# Join Predictions with Growth Rate Data
predictions_df = predictions.join(region_pivot, on="region", how="left")
predictions_df = predictions_df.join(df2020, on="region", how="left")

# Add Growth and Regression Predictions
predictions_df = predictions_df.withColumn("Regression_Prediction_2030", format_number("prediction", 0))
predictions_df = predictions_df.withColumn("Growth_Prediction_2030", format_number("Growth_Prediction_2030", 0))

# Blend Both Predictions (Averaging Both Approaches)
predictions_df = predictions_df.withColumn("Final_2030_Prediction", 
                                           (col("prediction") + col("Growth_Prediction_2030")) / 2)

# Select Final Output Columns
final = predictions_df.select(
    "region",
    format_number("TotalPopulation", 0).alias("2020_Population"),
    "Regression_Prediction_2030",
    "Growth_Prediction_2030",
    format_number("Final_2030_Prediction", 0).alias("Blended_2030_Prediction")
)

# Show Final Results
final.show()

# Save to CSV
final.write.csv("/user/jarrod/growth.csv", header=True)

# Stop Spark
spark.stop()
