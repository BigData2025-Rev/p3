from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CombineStatesToORC").config('spark.master', 'local[*]').getOrCreate()
path = "hdfs://localhost:9000/user/test/*.csv"

df = spark.read.csv(path, header=True, inferSchema=True)

orc_path = "hdfs://localhost:9000/user/test/2010_combined_states"

df.coalesce(1).write.format('orc').mode('overwrite').save(orc_path)

spark.stop()

