from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CombineStatesToORC").config('spark.master', 'local[*]').getOrCreate()

# Path to the folder containing the CSV files with data from each state.
path = "hdfs://localhost:9000/user/test/*.csv"

# Reads all the CSVs in the folder and loads them into a DataFrame with headers. Infers the schema and automatically assigns them appropriately.
df = spark.read.csv(path, header=True, inferSchema=True)

# Path to the folder where the output will be stored.
orc_path = "hdfs://localhost:9000/user/test/2010_combined_states"

# Coalescing the partitions to make the final ORC file useable.
df.coalesce(1).write.format('orc').mode('overwrite').save(orc_path)

spark.stop()
