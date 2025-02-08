import math
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

spark = SparkSession.builder\
    .appName("p3_analyze")\
    .config("spark.master", "local[*]")\
    .config("spark.sql.warehouse.dir", 'hdfs://localhost:9000/user/hive/warehouse')\
    .config("javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver")\
    .config("spark.driver.extraClassPath", '/home/tin/apache-hive-3.1.3-bin/lib/mysql-connector-java-8.0.30.jar') \
    .config("spark.executor.extraClassPath", '/home/tin/apache-hive-3.1.3-bin/lib/mysql-connector-java-8.0.30.jar') \
    .enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.sparkContext.getConf().getAll()

# Create table out of clean_data.orc
# spark.read.orc("/user/hive/warehouse/clean_data.orc").write.mode("overwrite").saveAsTable("p3")

df = spark.sql("""
                SELECT year, state_abbr, summary_level, name,
                    total_population, white_population, black_population,
                    american_indian_population, asian_population, native_hawaiian_population,
                    other_race_population, two_or_more_races_population
                FROM p3 
                WHERE summary_level = 40 OR summary_level = 500
                    AND (state_abbr, name) IN (
                        SELECT state_abbr, name
                        FROM p3
                        GROUP BY state_abbr, name
                        HAVING COUNT(*) == 3
                    )
            """)
print(df.count())

def cal_diversity_index(total, *args):
    # Sanity check
    if sum(args) != total:
        raise Exception()

    result = 0
    for pop in args:
        if pop <= 0:
            continue
        percent_pop = pop/total
        log_pop = math.log(percent_pop)
        result += percent_pop * log_pop

    result = -result
    return round(result, 2)

cal_diversity_index_udf = F.udf(cal_diversity_index, FloatType())
df = df.withColumn("diversity_index",cal_diversity_index_udf(
                                                                df["total_population"],
                                                                df["white_population"],
                                                                df["black_population"],
                                                                df["american_indian_population"],
                                                                df["asian_population"],
                                                                df["native_hawaiian_population"],
                                                                df["other_race_population"],
                                                                df["two_or_more_races_population"]
                                                            ))
df.show(2, truncate=False)

pivot_df = df.groupBy("state_abbr", "summary_level", "name")\
            .pivot("year")\
            .agg(
                F.first("diversity_index").alias("d_index"),
                F.first("total_population").alias("pop")
            )

pivot_df.show(2, truncate=False)

pivot_df = pivot_df.withColumn("00_10_growth",
                        F.round((F.col("2010_pop") - F.col("2000_pop") )/F.col("2000_pop"), 2))\
                    .withColumn("10_20_growth",
                        F.round((F.col("2020_pop") - F.col("2010_pop") )/F.col("2010_pop"), 2))

pivot_df = pivot_df.drop("2000_pop", "2010_pop", "2020_pop")
pivot_df = pivot_df.sort("state_abbr", "name")
pivot_df.show(2, truncate=False)

pivot_df.coalesce(1).write.mode("overwrite").csv("/user/tin/p3_final", header=True)