from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Convert CSV to ORC") \
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
input_dir = r"/mnt/c/Users/adeel/Desktop/combined_csv"
state_orc = r"/mnt/c/Users/adeel/Desktop/state_orc"

for state_folder in os.listdir(input_dir):
    state_path = os.path.join(input_dir, state_folder)

    # iterate and convert csv to orc for each state
    if os.path.isdir(state_path):
        state_dfs = []

        for file in os.listdir(state_path):
            if file.endswith(".csv"):
                file_path = os.path.join(state_path, file)

                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                state_dfs.append(df)

        if state_dfs:
            state_df = state_dfs[0]
            for other_df in state_dfs[1:]:
                state_df = state_df.union(other_df)

            state_df = state_df.coalesce(1)

            state_orc_output_path = os.path.join(state_orc, state_folder)
            state_df.write.orc(state_orc_output_path, mode="overwrite")

            print(f"ORC file: {state_orc_output_path}")
