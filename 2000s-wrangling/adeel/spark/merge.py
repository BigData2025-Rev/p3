from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit
import os
import glob
from functools import reduce

spark = SparkSession.builder.appName("Merge ORC Files") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.sql.files.maxPartitionBytes", "512m") \
    .config("spark.sql.files.openCostInBytes", "10485760") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxHeapSize=12g") \
    .getOrCreate()

# Paths
intermediate_orc_dir = r"/mnt/c/Users/adeel/Desktop/state_orc"
final_orc_path = r"/mnt/c/Users/adeel/Desktop/final_orc"

orc_files = []
for state_folder in os.listdir(intermediate_orc_dir):
    state_path = os.path.join(intermediate_orc_dir, state_folder)

    # looks for orc file in each state directory
    if os.path.isdir(state_path):
        orc_file = glob.glob(os.path.join(state_path, "part-*.orc"))
        if orc_file:
            orc_files.append(orc_file[0])

# store orc files (as dataframes) into a list
state_dfs = [spark.read.orc(file) for file in orc_files]

if state_dfs:
    # merge by union seperated dfs for each state
    combined_df = reduce(lambda df1, df2: df1.union(df2), state_dfs)

    # logrecno transformation -> state inital (STUSAB) + logrecno to make unique for each record
    combined_df = combined_df.withColumn("composite_key", concat(lit("2000"), lit("-"), col("STUSAB"), lit("-"), col("LOGRECNO")))
    combined_df = combined_df.coalesce(1)

    # write as orc file
    combined_df.write.orc(final_orc_path, mode="overwrite")

    print(f"ORC file created")