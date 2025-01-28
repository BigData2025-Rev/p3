from pyspark.sql import SparkSession

"""
quick check orc and csv files.
"""

spark = SparkSession.builder \
    .appName("Read ORC and CSV Files") \
    .config("spark.master", "local[4]") \
    .getOrCreate()

orc_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/orc/2000_combined_states.orc" 
csv_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/csv/2000_combined_states.csv"

orc_df = spark.read.orc(orc_file_path)
orc_row_count = orc_df.count()
orc_col_count = len(orc_df.columns)

csv_df = spark.read.option("header", "true").csv(csv_file_path)
csv_row_count = csv_df.count()
csv_col_count = len(csv_df.columns)

print(f"ORC File: {orc_file_path}")
print(f" - Rows: {orc_row_count}")
print(f" - Columns: {orc_col_count}")

print(f"CSV File: {csv_file_path}")
print(f" - Rows: {csv_row_count}")
print(f" - Columns: {csv_col_count}")

spark.stop()
