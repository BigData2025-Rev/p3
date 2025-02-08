# Jarrod  What regions will have the highest population in 2030?
# 	SUMLEV 40, Region, GroupBy(Year, Region)
# 	Linear regression on regions, predict 2030 value. Either plot or display in Map.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql.functions import format_number
from pyspark.sql.window import Window







def modelsummary(model):
    """Prints out the summary of the model"""
    import numpy as np
    print ("Note: the last rows are the information for Intercept")
    print ("##","-------------------------------------------------")
    print ("##"," Estimate | Std.Error | t Values | P-value")
    coef = np.append(list(model.coefficients),model.intercept)
    Summary=model.summary
    for i in range(len(Summary.pValues)):
        print ("##",'{:10.6f}'.format(coef[i]), \
               '{:10.6f}'.format(Summary.coefficientStandardErrors[i]), \
               '{:8.3f}'.format(Summary.tValues[i]), \
               '{:10.6f}'.format(Summary.pValues[i]))
    print ("##",'---')
    print ("##","Mean squared error: % .6f" \
           % Summary.meanSquaredError, ", RMSE: % .6f" \
           % Summary.rootMeanSquaredError )
    print ("##","Multiple R-squared: %f" % Summary.r2, ", \
    Total iterations: %i"% Summary.totalIterations)




# Initialize Spark Session
spark = SparkSession.builder.appName("PopulationPredictionsByRegion").getOrCreate()


# Read From Hdfs
df = spark.read.orc("/user/jarrod/clean_data.orc")

# Filter Out and group by region
data = df.filter(col("summary_level") == 40).select("year", "region", "total_population").dropna()
region_df = data.groupBy("year", "region").agg(sum("total_population").alias("TotalPopulation"))

# Index Region
indexer = StringIndexer(inputCol="region", outputCol="regionIndex")
region_df = indexer.fit(region_df).transform(region_df)



# Create Features and Label
assembler = VectorAssembler(inputCols=["year", "regionIndex"], outputCol="features")
region_df = assembler.transform(region_df)



# Train Linear Regression Model
lr = LinearRegression(featuresCol="features", labelCol="TotalPopulation", regParam=0.1)
model = lr.fit(region_df)
modelsummary(model)

# Predict 2030
regions = region_df.select("region").distinct()
future_data = regions.withColumn("year", lit(2030))


indexer_model = indexer.fit(future_data) 
future_data = indexer_model.transform(future_data)  
future_data = assembler.transform(future_data)


predictions = model.transform(future_data)

# Gather Predictions and extract
df2020 = region_df.filter(col("year") == 2020)
predictions_df = predictions.join(region_df, on="region")
predictions_df = predictions_df.withColumn("2030_Prediction", format_number("prediction", 2))
predictions_df = predictions_df.withColumn("2020_Population", format_number("TotalPopulation", 2))
final = predictions_df.drop("features","regionIndex","TotalPopulation","prediction","year")
final.show()
final.write.csv("/user/jarrod/p.csv", header=True)


spark.stop()
