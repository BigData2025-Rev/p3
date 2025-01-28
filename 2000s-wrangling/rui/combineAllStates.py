from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, from_json, explode_outer, monotonically_increasing_id
from pyspark.sql.functions import lit, concat
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.types import StructType
import os
import glob

# state_csv_files_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state_test" # if you want to test, better run this in a small test dir first
state_csv_files_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state"
state_output_combined_path_csv = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/csv/" # be careful, without file://, result will be write to hdfs
state_output_combined_path_orc = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/orc/"

# .config("spark.master", "local[4]") 限制并行度为4核，避免资源耗尽
# .config("spark.sql.shuffle.partitions", "200")  # 控制Shuffle分区数
# .config("spark.local.dir", "/mnt/c/.../spark-temp")  # 指向大容量磁盘的临时目录
# .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # 禁用Broadcast Join
# .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  # 启用高效GC

spark = SparkSession.builder \
    .appName("CombineStateCSVs") \
    .config("spark.master", "local[4]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.local.dir", "/tmp/spark-temp/") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()


# state_folders = [f.path for f in os.scandir(state_csv_files_path) if f.is_dir()]
# state_csv_path = []

# for state_folder in state_folders:
#     state_name = os.path.basename(state_folder)
#     files = glob.glob(os.path.join(state_folder, "part-*.csv"))
#     if files:
#         state_csv_path.append(files[0])

# print(state_csv_path)

# directly retrieve all csv files
csv_files = glob.glob(f"{state_csv_files_path}/*/part-*.csv")
csv_files = [f"file://{file}" for file in csv_files]
print("Retrived all csv files.", len(csv_files))


# read the schema of the first csv asa the sample schema, then apply to the rest csv files
sample_df = spark.read.option("header", "true").csv(csv_files[0])
csv_schema = sample_df.schema
print("Got sample schema.")

print(csv_schema)

# read all csv files with same schema
df = spark.read.schema(csv_schema) \
    .option("header", "true") \
    .csv(csv_files)

df = df.coalesce(1).persist(StorageLevel.MEMORY_AND_DISK)

# write to csv
os.makedirs(state_output_combined_path_csv, exist_ok=True)
df.write.option("header", "true") \
    .mode("overwrite") \
    .csv(f"file://{state_output_combined_path_csv}")

# write as orc
os.makedirs(state_output_combined_path_orc, exist_ok=True)
df.write.mode("overwrite").orc(f"file://{state_output_combined_path_orc}")

spark.stop()

print(f"combine finished, csv file under: {state_output_combined_path_csv}")
print(f"combine finished, csv file under: {state_output_combined_path_orc}")