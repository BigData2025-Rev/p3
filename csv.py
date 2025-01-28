from pyspark.sql import SparkSession
import os

# States
states = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Working Set
sub_states = [
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", 
    "Michigan", "Minnesota", "Mississippi", "Missouri"
]

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AppendCSVFiles") \
    .config("spark.master", "local[*]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .getOrCreate()

# Get all csv file paths
csv_path = []
for state in sub_states:
    path = f"/home/jarrod/uscensus/agg-states/{state}/{state}.csv"
    csv_path.append(path)


# Create Dataframe
df = [spark.read.csv(path, header = True, inferSchema = True) for path in csv_path]

# If no valid CSV files, stop the execution
if not df:
    print("No valid CSV files to process.")
    spark.stop()
    exit()

    
# Combine dataframe
combined_df = df[0]
for data in df[1:]:
    combined_df = combined_df.union(data)

# Write to output
combined_df.repartition(1).write.orc("/home/jarrod/uscensus/final/", mode = "overwrite")

spark.stop()

