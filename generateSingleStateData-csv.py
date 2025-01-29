from pyspark.sql import SparkSession

# change to your file path
stateFileLoc = "/Users/henrylee/Projects-Revature/Project3_USCensusData/"

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

# generate header for the p01 file
def generate_header_p01():
    header = ["FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO"]
    p_part1 = "P001"
    for num in range(1, 72):
        col_name = p_part1 + "{:04d}".format(num)
        header.append(col_name)

    p_part2 = "P002"
    for num in range(1, 74):
        col_name = p_part2 + "{:04d}".format(num)
        header.append(col_name)

    return header


def generate_header_p02():
    # create a header coloumns for the new dataframe
    header = ["FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO"]

    p_part3 = "P003"
    for num in range(1, 72):
        col_name = p_part3 + "{:04d}".format(num)
        header.append(col_name)

    p_part4 = "P004"
    for num in range(1, 74):
        col_name = p_part4 + "{:04d}".format(num)
        header.append(col_name)

    header.append("H0010001")
    header.append("H0010002")
    header.append("H0010003")

    return header


# read the p01 file and write to csv with header
def generate_df_with_header(input_file_name, schema):
    # read the csv file
    input_file = stateFileLoc + "data-files/" + input_file_name
    df = spark.read.csv(input_file)
    #df.show()

    # create a new dataframe with the extracted data and header columns
    new_df = spark.createDataFrame(df.rdd, schema)
    #df.show()

    new_df = new_df.repartition(1)  # Repartition to a single partition
    return new_df
    

def combine_csv_files(df1, df2):
    # Combine the two dataframes
    df2 = df2.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    combined_df = df1.join(df2, ["LOGRECNO"])

    # Convert to CSV
    output_file = stateFileLoc + "state_data.csv"
    combined_df.write.mode("overwrite").csv(output_file, header=True)
    

# input files to combine
schema_01 = generate_header_p01()
df_p01 = generate_df_with_header("ca000012010.csv", schema_01)

schema_02 = generate_header_p02()
df_p02 = generate_df_with_header("ca000022010.csv", schema_02)

combine_csv_files(df_p01, df_p02)

spark.stop()
