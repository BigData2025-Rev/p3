from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, from_json, explode_outer, monotonically_increasing_id
from pyspark.sql.functions import lit, concat
from pyspark.sql.types import ArrayType, StringType
import os

state_csv_files_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/output/"
state_output_combined_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state/"

spark = SparkSession.builder \
    .appName("DataProcessing") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.shuffle.file.buffer", "64k") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# combine three csv files
def combine_state_data(geo_df, file01_df, file02_df):
    file01_df = file01_df.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    file02_df = file02_df.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    combined_df = geo_df.join(file01_df, on="LOGRECNO", how="outer") \
                        .join(file02_df, on="LOGRECNO", how="outer")
    
    # remove the duplicate column
    combined_df = combined_df.drop(*[col for col in combined_df.columns if col.endswith('_1') or col.endswith('_2')])
    # add custom year column
    combined_df = combined_df.withColumn("Custom_Decade", lit(2000))
    # add unique column with 2000-{STUSAB}-{LOGRECNO}
    combined_df = combined_df.withColumn("Custom_Unique_Key",
                                         concat(lit("2000-"), col("STUSAB"), lit("-"), col("LOGRECNO")))

    return combined_df

# read the csv file for each state
def read_state_data(state_dir, output_path):
    geo_path = f"{state_dir}/geo.csv"
    file01_path = f"{state_dir}/00001.csv"
    file02_path = f"{state_dir}/00002.csv"

    if not os.path.exists(geo_path) or not os.path.exists(file01_path) or not os.path.exists(file02_path):
        print(f"Missing files in {state_dir}, skipping.")
        return

    geo_df = spark.read.csv(f"file://{geo_path}", header=True, inferSchema=True)
    file01_df = spark.read.csv(f"file://{file01_path}", header=True, inferSchema=True)
    file02_df = spark.read.csv(f"file://{file02_path}", header=True, inferSchema=True)

    # combine
    combined_df = combine_state_data(geo_df, file01_df, file02_df)
    combined_df.coalesce(1).write.csv(f"file://{output_path}", header=True, mode="overwrite")

    print(f"Combined data saved to: {output_path}")

    # release memory
    del geo_df, file01_df, file02_df, combined_df
    spark.catalog.clearCache()

state_folders = [f.path for f in os.scandir(state_csv_files_path) if f.is_dir()]

for state_folder in state_folders:
    state_name = os.path.basename(state_folder)
    output_path = f"{state_output_combined_path}/{state_name}"
    read_state_data(state_folder, output_path)

spark.stop()