from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
"""
quick check orc and csv files.
"""

spark = SparkSession.builder \
    .appName("Read ORC and CSV Files") \
    .config("spark.master", "local[4]") \
    .getOrCreate()

us_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/US_Summary/combined_each_state/0US_Summary/part-00000-bcd7bf1f-6208-4dc9-9e6e-b3dba313eff4-c000.csv" 
output_path_csv = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/US_Summary/filtered/csv/" 
output_path_orc = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/US_Summary/filtered/orc/" 

us_df = spark.read.option("header", "true").csv(us_path)
us_df.printSchema()

# only want data records before pacific
new_us_df = us_df.filter(col("LOGRECNO").cast("int") <= 14)

# write to the output path
os.makedirs(output_path_csv, exist_ok=True)
os.makedirs(output_path_orc, exist_ok=True)
new_us_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path_csv)
new_us_df.coalesce(1).write.mode("overwrite").orc(output_path_orc)

spark.stop()
