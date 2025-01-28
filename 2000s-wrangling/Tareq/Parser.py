from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
        .appName("Combine Headers and Values") \
        .master("local[*]") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .getOrCreate()

def parse_fixed_width_file(header_file, data_file, output_file):
    """
    Reads a fixed-width file (data_file) based on column information in header_file,
    and writes the parsed data into a CSV file.

    :param header_file: Path to the header CSV file containing column names and lengths.
    :param data_file: Path to the fixed-width data file.
    :param output_file: Path to save the resulting CSV file.
    """
    # Read the header CSV to get column names and lengths
    header_schema = StructType([
        StructField("DESC", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("LEN", IntegerType(), True),
    ])
    header_df = spark.read.csv(f"file://{header_file}", schema=header_schema, header=True)
 

      # Extract column names and lengths
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

    # Read the header file to extract column names
    with open(header_file, 'r') as f:
        headers = f.readline().strip().split(",")  # Extract the first line as column names

    # Read the values file as a DataFrame
    values_df = spark.read.csv(f"file://{value_file}", header=False)

    # Assign the headers as column names
    values_df = values_df.toDF(*headers)

    # Save the resulting DataFrame to a CSV file in the local file system
    values_df.coalesce(1).write.csv(f"file://{output_file}", header=True, mode="overwrite")

    print(f"Combined CSV saved to: {output_file}")

    # Stop the Spark session
    spark.stop()

# File paths
header_file1_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/0File_Structure/PL_Part1.csv"
value_file1_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/California/ca00001.upl"
output_file1_path = "/mnt/c/Users/ttaar/p2Dataset/part1_output.csv"

header_file2_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/0File_Structure/PL_Part2.csv"
value_file2_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/California/ca00002.upl"
output_file2_path = "/mnt/c/Users/ttaar/p2Dataset/part2_output.csv"

# Run the function
combine_columns_and_values(header_file1_path, value_file1_path, output_file1_path)
combine_columns_and_values(header_file2_path, value_file2_path, output_file2_path)


header_file_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/0File_Structure/header.csv"
data_file_path = "/mnt/c/Users/ttaar/p2Dataset/redistricting_file--pl_94-171/California/cageo.upl"
output_file_path = "/mnt/c/Users/ttaar/p2Dataset/geo_output.csv"

# Parse the fixed-width file and save the output
parse_fixed_width_file(header_file_path, data_file_path, output_file_path)
