import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, abs

spark = SparkSession.builder\
    .appName("ReadCleanedDataset")\
    .master("local[*]")\
    .config("spark.hadoop.fs.defaultFS", "file:///")\
    .getOrCreate()

"""
  Question: What will be the districts with the highest populations in 2030?
  1. First get all districts (sumlev=500) for each year from origional table
  2. Since I do not have any data from 2030 for model training,
    the only information I know for 2030 decades are: year, state, name, region
    I choose to:
    set year, state, name as input columns and transform them.
    set total_population as output column.
  3. transform all non-numerical columns with one hot encoded
  4. divide dataset to training set and testing set with ratio 80:20
  5. train the liner regression model with training set
  6. test the linear regession model in testing set
  7. predict the 2030 population for each district
"""
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# take population data for each decade for each district
df = df_origional.filter(col("summary_level")==500).select("year", "state_abbr", "name", "region", "total_population")

# transform non-numerical columns to numerical type and the ntransfer to one-hot code
# year and total_population are already numerical columns and I  want them remain numerical for comparison
categorical_columns = ["year", "state_abbr", "name", "region"]
transform_columns = ["state_abbr", "name", "region"]
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in transform_columns]
encoder = OneHotEncoder(inputCols=[indexer.getOutputCol() for indexer in indexers], outputCols=[column+"_onehot" for column in transform_columns])
assembler = VectorAssembler(inputCols=["year"] + [column + "_onehot" for column in transform_columns], outputCol="features")

# create pipline to assemble all transform stages, apply to the dataset
stages = []
stages.extend(indexers)
stages.append(encoder)
stages.append(assembler)
pipeline = Pipeline(stages=stages)
pipeline_model = pipeline.fit(df)
df_transformed = pipeline_model.transform(df)

# split the data to training set and testing set
train_data, test_data = df_transformed.randomSplit([0.8, 0.2], seed=20250203)

# train the model, then test on testing set
lr = LinearRegression(featuresCol="features", labelCol="total_population")
lr_model = lr.fit(train_data)
prediction = lr_model.transform(test_data)
prediction.select("year", "state_abbr", "name", "region", "total_population", "prediction").show(10)

# Evulate the model with root mean square error since we are predicting numerical values
evulator_rmse = RegressionEvaluator(labelCol="total_population", predictionCol="prediction", metricName="rmse")
rmse = evulator_rmse.evaluate(prediction)
print("Root Mean Squared Error on test data = %g" % rmse)

# Evulate R2 score
evulator_rmse = RegressionEvaluator(labelCol="total_population", predictionCol="prediction", metricName="r2")
rmse = evulator_rmse.evaluate(prediction)
print("R squared on test data = %g" % rmse)


"""
Predict the data for 2030
"""
# transform the columns
districts_df = df.select("state_abbr", "name", "region").distinct()
districts_df = districts_df.withColumn("year", lit(2030))
districts_2030_transformed = pipeline_model.transform(districts_df)
# predict and record in dataset
predictions_2030 = lr_model.transform(districts_2030_transformed)
predictions_2030 = predictions_2030.withColumn("distinct_id", concat(col("state_abbr"), lit("-"),col("name")))
df = df_origional.filter(col("summary_level")==500)

# select the population from 2000~2020, and join with 2030 prediction for comprasion
df_2000 = df.filter(col("year")==2000).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2000")
df_2010 = df.filter(col("year")==2010).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2010")
df_2020 = df.filter(col("year")==2020).select("state_abbr", "name", "total_population").withColumnRenamed("total_population", "total_population_2020")
df_joined = df_2000.join(df_2010, ["state_abbr", "name"], "inner").join(df_2020, ["state_abbr", "name"], "inner")
predictions_2030 = predictions_2030.join(df_joined, ["state_abbr", "name"], "inner")
predictions_2030 = predictions_2030.withColumnRenamed("prediction", "prediction_population_2030")

predictions_2030.printSchema()
predictions_2030 = predictions_2030.select("year", "distinct_id", "state_abbr", "name", "region", "prediction_population_2030", "total_population_2000", "total_population_2010", "total_population_2020").withColumnRenamed("prediction", "total_population")
predictions_2030.show(10)
predictions_2030.write.mode("overwrite").option("header", "true").csv("/content/predictions_2030/")

spark.stop()