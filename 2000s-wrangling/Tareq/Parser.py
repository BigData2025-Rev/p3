import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark
spark = SparkSession.builder \
        .appName("Process UPL Files for All States") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

def parse_fixed_width_file(header_file, data_file, output_file):
    """
    Reads a fixed-width file (data_file) based on column information in header_file,
    and writes the parsed data into a CSV file.
    """
    header_schema = StructType([
        StructField("DESC", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("LEN", IntegerType(), True),
    ])
    header_df = spark.read.csv(f"file://{header_file}", schema=header_schema, header=True)

    column_names = [row["NAME"] for row in header_df.collect()]
    column_lengths = [row["LEN"] for row in header_df.collect()]

    def parse_fixed_width_line(line):
        positions = [0] + column_lengths
        positions = [sum(positions[:i + 1]) for i in range(len(positions))]
        return [line[positions[i]:positions[i + 1]].strip() for i in range(len(positions) - 1)]

    data_rdd = spark.sparkContext.textFile(data_file).map(parse_fixed_width_line)
    data_df = data_rdd.toDF(column_names)

    data_df.coalesce(1).write.csv(f"file://{output_file}", header=True, mode="overwrite")


def combine_columns_and_values(header_file, value_file, output_file):
    """
    Reads a CSV header file and a values file, then saves the formatted DataFrame.
    """
    with open(header_file, 'r') as f:
        headers = f.readline().strip().split(",")

    values_df = spark.read.csv(f"file://{value_file}", header=False)
    values_df = values_df.toDF(*headers)

    values_df.coalesce(1).write.csv(f"file://{output_file}", header=True, mode="overwrite")


def process_all_states(base_path):
    """
    Iterates through all state directories in the dataset folder and processes `.upl` files.
    """
    state_dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

    # Paths for header files (unchanged)
    header_part1 = os.path.join(base_path, "0File_Structure", "PL_Part1.csv")
    header_part2 = os.path.join(base_path, "0File_Structure", "PL_Part2.csv")
    header_geo = os.path.join(base_path, "0File_Structure", "header.csv")

    for state in state_dirs:
        state_path = os.path.join(base_path, state)

        # **Find all `.upl` files inside each state directory**
        upl_files = [f for f in os.listdir(state_path) if f.endswith(".upl")]

        for upl_file in upl_files:
            full_file_path = os.path.join(state_path, upl_file)
            
            if "geo" in upl_file.lower():  # **Process Geography file**
                geo_output = os.path.join("/mnt/c/Users/ttaar/p2Dataset/", f"{state}_geo_output.csv")
                parse_fixed_width_file(header_geo, full_file_path, geo_output)
                print(f"Processed GEO file for {state}")

            elif "01" in upl_file.lower():  # **Process PL_Part1**
                part1_output = os.path.join("/mnt/c/Users/ttaar/p2Dataset/", f"{state}_part1_output.csv")
                combine_columns_and_values(header_part1, full_file_path, part1_output)
                print(f"Processed Part1 file for {state}")

            elif "02" in upl_file.lower():  # **Process PL_Part2**
                part2_output = os.path.join("/mnt/c/Users/ttaar/p2Dataset", f"{state}_part2_output.csv")
                combine_columns_and_values(header_part2, full_file_path, part2_output)
                print(f"Processed Part2 file for {state}")

        print(f"✅ Finished processing state: {state}")


# Base directory where states are stored
base_dataset_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171"

# Process all state folders dynamically
process_all_states(base_dataset_path)

# Stop Spark session
spark.stop()
