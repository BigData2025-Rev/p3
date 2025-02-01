import os
import SparkSession

# Init Spark Session
spark = SparkSession.builder.appName("combine") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.memoryOverhead", "4g") \
    .config("spark.driver.maxResultSize", "6g") \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.sql.files.maxPartitionBytes", "512m") \
    .config("spark.sql.files.openCostInBytes", "10485760") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.debug.maxToStringFields", "200") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxHeapSize=12g") \
    .getOrCreate()

class Combine:
    def combine_state(file_00001, file_00002, file_geo, output_file):
        """
        HELPER FUNCTION - Combines 3 State CSV into a single State CSV
        DROP COL - Drops duplicated "FILEID", "STUSAB", "CHARITER", "CIFSN" cols
        MERGE - Merges on LOGRECNO col then drops duplicates of it

        file_00001 - Part of state upl (converted to csv via parser) files
        file_00002 - Part of state upl (converted to csv via parser) files
        file_geo - Part of state upl (converted to csv via parser) files
        output_file - output single state csv (ex. California_combined.csv)
        """

        # Read CSV via Spark
        df_00001 = spark.read.csv(file_00001, header=True, inferSchema=True)
        df_00002 = spark.read.csv(file_00002, header=True, inferSchema=True)
        df_geo = spark.read.csv(file_geo, header=True, inferSchema=True)

        # Drop duplicated cols (aside form LOGRECNO which will be used to merge and then dropped)
        cols_to_drop = ["FILEID", "STUSAB", "CHARITER", "CIFSN"]
        df_00002 = df_00002.drop(*cols_to_drop)
        df_geo = df_geo.drop(*cols_to_drop)

        # Join the dataframes on LOGRECNO
        combined_df = (
            df_00001
            .join(df_00002, on="LOGRECNO", how="left")
            .join(df_geo, on="LOGRECNO", how="left")
        )

        # Drop duplicate LOGRECNO columns
        combined_df = combined_df.dropDuplicates()

        # Ensure single CSV file (coalesce) and write to output path
        combined_df.coalesce(1).write.csv(output_file, mode="overwrite", header=True)
        print(f"Combined file saved to: {output_file}")

    def combine(input_dir, output_dir):
        """
        Iterates through aLL state directories and uses helper function to convert
        3 csv files into a single csv file to then be saved to output_dir
        Creates a state folder for each state and puts single csv file into there

        input_dir - file with state folders containing 3 csvs each
        output_dir - file to output state folders with 1 csv each
        """
        for state_folder in os.listdir(input_dir):
            # State path inside input_dir
            state_path = os.path.join(input_dir, state_folder)

            # Checks to ensure state_path exists
            if os.path.isdir(state_path):
                file_00001 = None
                file_00002 = None
                file_geo = None
                
                # finds each file by ending
                for file in os.listdir(state_path):
                    if file.endswith("00001.csv"):
                        file_00001 = os.path.join(state_path, file)
                    elif file.endswith("00002.csv"):
                        file_00002 = os.path.join(state_path, file)
                    elif file.endswith("geo.csv"):
                        file_geo = os.path.join(state_path, file)
                
                # Ensures each file exists and uses helper function to combine state files
                if file_00001 and file_00002 and file_geo:
                    output_state_dir = os.path.join(output_dir, state_folder)
                    # ensures output state dir (just created) exists or creates
                    os.makedirs(output_state_dir, exist_ok=True)
                    output_file = os.path.join(output_state_dir, f"{state_folder}_combined")
                    Combine.combine_state(file_00001, file_00002, file_geo, output_file)
                else:
                    print(f"Missing files for state folder: {state_folder}")
class Convert:
    def convert(input_dir, output_dir):
        """
        Iterates through aLL state directories and converts single csv to orc
        Maintains schema and datatypes

        input_dir - file with state folders containing 1 csv each
        output_dir - file to output state folders with 1 orc each
        """
        for state_folder in os.listdir(input_dir):
            state_path = os.path.join(input_dir, state_folder)

            # iterate and convert csv to orc for each state
            if os.path.isdir(state_path):
                state_dfs = []

                for file in os.listdir(state_path):
                    if file.endswith(".csv"):
                        file_path = os.path.join(state_path, file)

                        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                        state_dfs.append(df)

                if state_dfs:
                    state_df = state_dfs[0]
                    for other_df in state_dfs[1:]:
                        state_df = state_df.union(other_df)

                    state_df = state_df.coalesce(1)

                    state_orc_output_path = os.path.join(output_dir, state_folder)
                    state_df.write.orc(state_orc_output_path, mode="overwrite")

                    print(f"Combined ORC file: {state_orc_output_path}")
