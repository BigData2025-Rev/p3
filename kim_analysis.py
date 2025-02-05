from pyspark.sql import SparkSession
from pyspark.sql.functions import greatest, least, when
spark = SparkSession.builder.appName("analysis").getOrCreate()

df = spark.read.orc("clean_data.orc")
df = df.filter(df.summary_level == 500)


df = df.withColumn("majority_count", greatest("white_population", "black_population", "american_indian_population", \
    "asian_population", "native_hawaiian_population", "other_race_population", "two_or_more_races_population"))
df = df.withColumn("majority_race", when(df.majority_count == df.white_population, "white")
                                    .otherwise(when(df.majority_count == df.black_population, "black")
                                    .otherwise(when(df.majority_count == df.american_indian_population, "american_indian")
                                    .otherwise(when(df.majority_count == df.asian_population, "asian")
                                    .otherwise(when(df.majority_count == df.native_hawaiian_population, "native_hawaiian")
                                    .otherwise(when(df.majority_count == df.other_race_population, "other_race")
                                    .otherwise("two_or_more_races")))))))

df = df.withColumn("minority_count", least("white_population", "black_population", "american_indian_population", \
    "asian_population", "native_hawaiian_population", "other_race_population", "two_or_more_races_population"))

#

df = df.withColumn("minority_race", when(df.minority_count == df.white_population, "white")
                                    .otherwise(when(df.minority_count == df.black_population, "black")
                                    .otherwise(when(df.minority_count == df.american_indian_population, "american_indian")
                                    .otherwise(when(df.minority_count == df.asian_population, "asian")
                                    .otherwise(when(df.minority_count == df.native_hawaiian_population, "native_hawaiian")
                                    .otherwise(when(df.minority_count == df.other_race_population, "other_race")
                                    .otherwise("two_or_more_races")))))))

df = df.dropna(subset=["district"])
df = df.filter(df.district != "")

df = df.withColumn("county", df.county.cast("int"))
df = df.withColumn("district", df.district.cast("int"))

df.write.csv("added_columns.csv", header=True, mode="overwrite")


countMajority = df.groupBy("year", "majority_race").count()
countMinority = df.groupBy("year", "minority_race").count()

countMajority.show()
countMinority.show()

countMajority.write.csv("countMajority.csv", header=True, mode="overwrite")




#Joins the 2000 and 2020 data to compare racial majorities
df2000 = df.filter(df.year == 2000)
df2000 = df2000.toDF(*[f"{c}_2000" for c in df2000.columns])
df2000.show()

df2020 = df.filter(df.year == 2020)
df2020 = df2020.toDF(*[f"{c}_2020" for c in df2020.columns])
df2020.show()

df2000 = df2000.withColumnRenamed("district_2000", "district")
df2000 = df2000.withColumnRenamed("state_abbr_2000", "state_abbr")

df2020 = df2020.withColumnRenamed("district_2020", "district")
df2020 = df2020.withColumnRenamed("state_abbr_2020", "state_abbr")





finaldf = df2000.join(df2020, on=["district", "state_abbr"], how="inner")
finaldf = finaldf.orderBy("total_population_2020", ascending=False)
#finaldf = finaldf.select("name", "majority_race_2000", "minority_race_2000", "majority_race_2020", "minority_race_2020")
finaldf = finaldf.filter(finaldf.majority_race_2000 != finaldf.majority_race_2020)
finaldf.show()




#output to csv
finaldf.write.csv("final_data.csv", header=True, mode="overwrite")



# #output to csv
# finaldf.write.csv("final_data.csv", header=True, mode="overwrite")
