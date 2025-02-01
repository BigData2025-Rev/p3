from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("queries").config("spark.master", "local[*]").getOrCreate()

file_path = "/mnt/c/users/ttaar/project3/2000_combined_states.orc"

dff = spark.read.orc(f"file://{file_path}")

print(dff.count())