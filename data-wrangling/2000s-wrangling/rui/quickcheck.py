from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, regexp_replace, from_json, explode_outer, monotonically_increasing_id

"""
quick check orc and csv files.
"""

spark = SparkSession.builder \
    .appName("Read ORC and CSV Files") \
    .config("spark.master", "local[4]") \
    .getOrCreate()

orc_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/orc/2000_combined_states.orc" 
csv_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates_test/csv/2000_combined_states.csv"

# orc_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/orc/2000_combined_states.orc" 
# csv_file_path = "file:///mnt/c/Personal_Official/Project/revature/project3/data/2000/allStates/csv/2000_combined_states.csv"

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

df = orc_df.select(orc_df.LOGRECNO, orc_df.SUMLEV, orc_df.NAME, orc_df.P0010001, orc_df.P0010003, orc_df.INTPTLAT, orc_df.INTPTLON).filter((col("STUSAB") == "PA") & (col("NAME") == "Allegheny County (part)")).sort("P0010001")

df = df.filter((col("STUSAB") == "PA") & (col("NAME") == "Allegheny County (part)")).drop("LOGRECNO", "INTPTLAT", "INTPTLON")

print(df.count())

df.collect()

df = df.distinct()
print(df.count())

spark.stop()
