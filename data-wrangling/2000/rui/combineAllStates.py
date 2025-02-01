from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
import os
import glob

# state_files_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state_test"        # if you want to test, better run this in a small test dir first
# state_output_combined_path_csv = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/csv/"   # test outpuit folder
# state_output_combined_path_orc = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/orc/"   # test output folder

# us_summary_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state_test/0US_Summary"
# us_summary_output_path_csv = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/US_Summary/csv/"
# us_summary_output_path_orc = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/US_Summary/orc/"

state_files_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state"
state_output_combined_path_csv = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/csv/" # be careful, without file://, result will be write to hdfs
state_output_combined_path_orc = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/orc/"

us_summary_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/combined_each_state/0US_Summary"
us_summary_output_path_csv = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/US_Summary/csv/"
us_summary_output_path_orc = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/US_Summary/orc/"

merge_type = "csv"  # change this to the type of the combine files you want to read. such as orc

# .config("spark.sql.shuffle.partitions", "200")            # change the shuffle partition number
# .config("spark.local.dir", "/mnt/c/.../spark-temp")       # specific the temp data folder
# .config("spark.sql.autoBroadcastJoinThreshold", "-1")     # ban Broadcast Join
# .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  # enable gc in high efficency

spark = SparkSession.builder \
    .appName("CombineStateCSVs") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.local.dir", "/tmp/spark-temp/") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
    .getOrCreate()

def mergeStates():
    state_folders = [f.path for f in os.scandir(state_files_path) if f.is_dir()]
    state_files = []

    for state_folder in state_folders:
        if "ummary" in state_folder:
            # to avoid read the US_Summary folder
            continue
        if merge_type == "csv":
            file = glob.glob(os.path.join(state_folder, "*.csv"))
        if merge_type == "orc":
            file = glob.glob(os.path.join(state_folder, "*.orc"))
        if file:
            state_files.append(file[0])

    print("Retrived all csv files.", len(state_files))

    # read all csv or orc files
    if merge_type == "orc":
        state_dfs = [spark.read.orc(file) for file in state_files]
    else:
        state_dfs = [spark.read.option("header", "true").csv(file) for file in state_files]

    # merge the state dataframes and coalesce to single file
    merged_df = reduce(lambda x, y: x.union(y), state_dfs).coalesce(1)
    # perhaps if we want to create new columns
    merged_df = merged_df.withColumn("LOGRECNO", concat(col("STUSAB"), col("LOGRECNO")))
    merged_df = merged_df.withColumn("Custom_Decade", lit(2000))
    merged_df = merged_df.withColumn("Custom_Unique_Key", 
                        concat(lit("2000-"), col("STUSAB"), lit("-"), col("LOGRECNO")))

    # make sure the output path exists
    os.makedirs(state_output_combined_path_csv, exist_ok=True)
    os.makedirs(state_output_combined_path_orc, exist_ok=True)

    # write to csv
    # write as orc
    merged_df.write.option("header", "true").mode("overwrite").csv(state_output_combined_path_csv)
    merged_df.write.mode("overwrite").orc(state_output_combined_path_orc)

    print(f"Merge finished, CSV file under: {state_output_combined_path_csv}")
    print(f"Merge finished, ORC file under: {state_output_combined_path_orc}")

def cleanUSSummary():
    """
    Clean the US Summary file and store as csv/orc
    """
    if merge_type == "csv":
        file = glob.glob(os.path.join(us_summary_path, "*.csv"))
        us_df = spark.read.option("header", "true").csv(file)
    if merge_type == "orc":
        file = glob.glob(os.path.join(us_summary_path, "*.orc"))
        us_df = spark.read.orc(file)
    
    # Only want the data records before pacific
    us_df = us_df.filter(col("LOGRECNO").cast("int") <= 14).coalesce(1)
    
    # make sure the column names remains the same as the state file
    us_df = us_df.withColumn("LOGRECNO", concat(col("STUSAB"), col("LOGRECNO")))
    us_df = us_df.withColumn("Custom_Decade", lit(2000))
    us_df = us_df.withColumn("Custom_Unique_Key", 
                        concat(lit("2000-"), col("STUSAB"), lit("-"), col("LOGRECNO")))
    
    # write the US file in both csv and orc format
    os.makedirs(us_summary_output_path_csv, exist_ok=True)
    os.makedirs(us_summary_output_path_csv, exist_ok=True)

    # write to csv
    # write as orc
    us_df.write.option("header", "true").mode("overwrite").csv(us_summary_output_path_csv)
    us_df.write.mode("overwrite").orc(us_summary_output_path_orc)

    print(f"US Summary file filtered, CSV file under: {us_summary_output_path_csv}")
    print(f"US Summary file filtered, ORC file under: {us_summary_output_path_orc}")


mergeStates()
cleanUSSummary()

spark.stop()