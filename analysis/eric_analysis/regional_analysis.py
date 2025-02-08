from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Read ORC File").getOrCreate()

df = spark.read.orc("clean_data.orc").select("unique_key", "year", "state_abbr", "summary_level","name", "region", "total_population", "white_population", "black_population", "american_indian_population", "asian_population", "native_hawaiian_population",  "two_or_more_races_population","other_race_population")

df.printSchema()
df.show(10)
print(f'DataFrame Row Count: {df.count()}')

df = df.filter(df['summary_level'] == 40)
df.show(10)
print(f'DataFrame Row Count: {df.count()}')

df1 = df.groupBy('year').agg(sum("total_population")).orderBy('year')
df2 = df.groupBy('year', 'region').agg(sum("total_population")).orderBy('region','year')
df1.show()
df2.show()

df3 = df.groupBy('year', 'region').agg(sum("total_population"), sum("white_population"), sum('black_population'), sum('american_indian_population'), sum('asian_population'), sum('native_hawaiian_population'), sum("two_or_more_races_population"), sum("other_race_population")).orderBy('region','year')
df3.show()

df1.coalesce(1).write.csv('total_us_pop.csv', header=True, mode="overwrite")
df2.coalesce(1).write.csv('total_us_pop_by_region.csv', header=True, mode="overwrite")
df3.coalesce(1).write.csv('total_us_pop_by_region_race.csv', header=True, mode="overwrite")

dfWest = df3.filter(df3['region'] == 'West')
dfWest.show()
dfWest.coalesce(1).write.csv('total_us_pop_by_region_race_West.csv', header=True, mode="overwrite")

dfSouth = df3.filter(df3['region'] == 'South')
dfSouth.show()
dfSouth.coalesce(1).write.csv('total_us_pop_by_region_race_South.csv', header=True, mode="overwrite")

dfMidwest = df3.filter(df3['region'] == 'Midwest')
dfMidwest.show()
dfMidwest.coalesce(1).write.csv('total_us_pop_by_region_race_Midwest.csv', header=True, mode="overwrite")

dfNortheast = df3.filter(df3['region'] == 'Northeast')
dfNortheast.show()
dfNortheast.coalesce(1).write.csv('total_us_pop_by_region_race_Northeast.csv', header=True, mode="overwrite")



spark.stop()