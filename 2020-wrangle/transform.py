from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat
import os

# Define paths
state_csv_files_path = "/home/jarrod/uscensus/states/"
state_output_combined_path = "/home/jarrod/uscensus/agg-states/"

# Define state abbreviation mapping
state_map = {
    'Illinois': 'il',
    'Indiana': 'in',
    'Iowa': 'ia',
    'Kansas': 'ks',
    'Kentucky': 'ky',
    'Louisiana': 'la',
    'Maine': 'me',
    'Maryland': 'md',
    'Massachusetts': 'ma',
    'Michigan': 'mi',
    'Minnesota': 'mn',
    'Mississippi': 'ms',
    'Missouri': 'mo'
}

# Initialize Spark session
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



file1_headers = [
    "FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO", 
    "P0010001", "P0010002", "P0010003", "P0010004", "P0010005",
    "P0010006", "P0010007", "P0010008", "P0010009", "P0010010", 
    "P0010011", "P0010012", "P0010013", "P0010014", "P0010015",
    "P0010016", "P0010017", "P0010018", "P0010019", "P0010020", 
    "P0010021", "P0010022", "P0010023", "P0010024", "P0010025",
    "P0010026", "P0010027", "P0010028", "P0010029", "P0010030",
    "P0010031", "P0010032", "P0010033", "P0010034", "P0010035", 
    "P0010036", "P0010037", "P0010038", "P0010039", "P0010040", 
    "P0010041", "P0010042", "P0010044", "P0010045", "P0010046", 
    "P0010047", "P0010048", "P0010049", "P0010050", "P0010051",
    "P0010052", "P0010053", "P0010054", "P0010055", "P0010056", 
    "P0010057", "P0010058", "P0010059", "P0010060", "P0010061",
    "P0010062", "P0010063", "P0010064", "P0010065", "P0010066", 
    "P0010067", "P0010068", "P0010069", "P0010070", "P0010071",
    "P0020001", "P0020002", "P0020003", "P0020004", "P0020005",
    "P0020006", "P0020007", "P0020008", "P0020009", "P0020010", 
    "P0020011", "P0020012", "P0020013", "P0020014", "P0020015",
    "P0020016", "P0020017", "P0020018", "P0020019", "P0020020",
    "P0020021", "P0020022", "P0020023", "P0020024", "P0020025", 
    "P0020026", "P0020027", "P0020028", "P0020029", "P0020030",
    "P0020031", "P0020032", "P0020033", "P0020034", "P0020035", 
    "P0020036", "P0020037", "P0020038", "P0020039", "P0020040", 
    "P0020041", "P0020042", "P0020043", "P0020044", "P0020045",
    "P0020046", "P0020047", "P0020048", "P0020049", "P0020050",
    "P0020051", "P0020052", "P0020053", "P0020054", "P0020055", 
    "P0020056", "P0020057", "P0020058", "P0020059", "P0020060", 
    "P0020061", "P0020062", "P0020063", "P0020064", "P0020065", 
    "P0020066", "P0020067", "P0020068", "P0020069", "P0020070", 
    "P0020071", "P0020072", "P0020073", "P0020074"
]
# Define geo_headers
geo_headers = [
    "FILEID", "STUSAB", "SUMLEV", "GEOVAR", "GEOCOMP", "CHARITER", "CIFSN", "LOGRECNO",
    "GEOID", "GEOCODE", "REGION", "DIVISION", "STATE", "STATENS", "COUNTY", "COUNTYCC",
    "COUNTYNS", "COUSUB", "COUSUBCC", "COUSUBNS", "SUBMCD", "SUBMCDCC", "SUBMCDNS", "ESTATE",
    "ESTATECC", "ESTATENS", "CONCIT", "CONCITCC", "CONCITNS", "PLACE", "PLACECC", "PLACENS",
    "TRACT", "BLKGRP", "BLOCK", "AIANHH", "AIHHTLI", "AIANHHFP", "AIANHHCC", "AIANHHNS", "AITS",
    "AITSFP", "AITSCC", "AITSNS", "TTRACT", "TBLKGRP", "ANRC", "ANRCCC", "ANRCNS", "CBSA", "MEMI",
    "CSA", "METDIV", "NECTA", "NMEMI", "CNECTA", "NECTADIV", "CBSAPCI", "NECTAPCI", "UA", "UATYPE",
    "UR", "CD116", "CD118", "CD119", "CD120", "CD121", "SLDU18", "SLDU22", "SLDU24", "SLDU26", "SLDU28",
    "SLDL18", "SLDL22", "SLDL24", "SLDL26", "SLDL28", "VTD", "VTDI", "ZCTA", "SDELM", "SDSEC", "SDUNI",
    "PUMA", "AREALAND", "AREAWATR", "BASENAME", "LSAD", "NAME", "FUNCSTAT", "GCUNI", "POP100", "HU100",
    "INTPTLAT", "INTPTLON", "LSADC", "PARTFLAG"
]

file2_headers = [
    "FILEID", "STUSAB", "CHARITER", "CIFSN", "LOGRECNO",
    "P0030001", "P0030002", "P0030003", "P0030004", "P0030005",
    "P0030006", "P0030007", "P0030008", "P0030009", "P0030010", 
    "P0030011", "P0030012", "P0030013", "P0030014", "P0030015",
    "P0030016", "P0030017", "P0030018", "P0030019", "P0030020", 
    "P0030021", "P0030022", "P0030023", "P0030024", "P0030025",
    "P0030026", "P0030027", "P0030028", "P0030029", "P0030030",
    "P0030031", "P0030032", "P0030033", "P0030034", "P0030035", 
    "P0030036", "P0030037", "P0030038", "P0030039", "P0030040", 
    "P0030041", "P0030042", "P0030043", "P0030044", "P0030045",
    "P0030046", "P0030047", "P0030048", "P0030049", "P0030050",
    "P0030051", "P0030052", "P0030053", "P0030054", "P0030055", 
    "P0030056", "P0030057", "P0030058", "P0030059", "P0030060", 
    "P0030061", "P0030062", "P0030063", "P0030064", "P0030065", 
    "P0030066", "P0030067", "P0030068", "P0030069", "P0030070", 
    "P0030071", "P0040001", "P0040002", "P0040003", "P0040004", 
    "P0040005","P0040006", "P0040007", "P0040008", "P0040009", 
    "P0040010", "P0040011", "P0040012", "P0040013", "P0040014", 
    "P0040015", "P0040016", "P0040017", "P0040018", "P0040019", 
    "P0040020", "P0040021", "P0040022", "P0040023", "P0040024", 
    "P0040025", "P0040026", "P0040027", "P0040028", "P0040029", 
    "P0040030", "P0040031", "P0040032", "P0040033", "P0040034", 
    "P0040035", "P0040036", "P0040037", "P0040038", "P0040039", 
    "P0040040", "P0040041", "P0040042", "P0040043", "P0040044", 
    "P0040045", "P0040046", "P0040047", "P0040048", "P0040049", 
    "P0040050", "P0040051", "P0040052", "P0040053", "P0040054", 
    "P0040055", "P0040056", "P0040057", "P0040058", "P0040059", 
    "P0040060", "P0040061", "P0040062", "P0040063", "P0040064", 
    "P0040065", "P0040066", "P0040067", "P0040068", "P0040069", 
    "P0040070", "P0040071", "P0040072", "P0040073", "H0010001", 
    "H0010002", "H0010003"
]


# Combine three CSV files
def combine_state_data(geo_df, file01_df, file02_df):
    # Drop unnecessary columns
    file01_df = file01_df.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    file02_df = file02_df.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")

    # Join dataframes on "LOGRECNO"
    combined_df = geo_df.join(file01_df, on="LOGRECNO", how="outer") \
                        .join(file02_df, on="LOGRECNO", how="outer")
    
    # Add custom columns
    combined_df = combined_df.withColumn("Custom_Decade", lit(2020))
    combined_df = combined_df.withColumn("Custom_Unique_Key",
                                         concat(lit("2020-"), col("STUSAB"), lit("-"), col("LOGRECNO")))

    return combined_df

# Read the CSV file for each state
def read_state_data(state_dir, output_path, state_name):
    geo_path = f"{state_dir}/{state_map[state_name]}2020.pl/{state_map[state_name]}geo2020.pl"
    file01_path = f"{state_dir}/{state_map[state_name]}2020.pl/{state_map[state_name]}000012020.pl"
    file02_path = f"{state_dir}/{state_map[state_name]}2020.pl/{state_map[state_name]}000022020.pl"
    print(file01_path)

    if not os.path.exists(geo_path) or not os.path.exists(file01_path) or not os.path.exists(file02_path):
        print(f"Missing files in {state_dir}, skipping.")
        return

    # Read the files without headers and manually assign column names
    geo_df = spark.read.csv(f"file://{geo_path}", header=False, inferSchema=True, sep="|")
    file01_df = spark.read.csv(f"file://{file01_path}", header=False, inferSchema=True, sep="|")
    file02_df = spark.read.csv(f"file://{file02_path}", header=False, inferSchema=True, sep="|")

    # Assign headers to each dataframe
    geo_df = geo_df.toDF(*geo_headers)
    file01_df = file01_df.toDF(*file1_headers)
    file02_df = file02_df.toDF(*file2_headers)

 
    # print("Geo DataFrame Headers:", geo_df.columns)
    # print("File01 DataFrame Headers:", file01_df.columns)
    # print("File02 DataFrame Headers:", file02_df.columns)

    # Combine the dataframes
    combined_df = combine_state_data(geo_df, file01_df, file02_df)
    
    # Remove duplicates
    combined_df = combined_df.dropDuplicates()
    
    # Save the combined data to the output path
    combined_df.coalesce(1).write.csv(f"file://{output_path}", header=True, mode="overwrite")

    print(f"Combined data saved to: {output_path}")

    #Release memory
    del geo_df, file01_df, file02_df, combined_df
    spark.catalog.clearCache()

# List the folders and process each state's data
state_folders = [f.path for f in os.scandir(state_csv_files_path) if f.is_dir()]

for state_folder in state_folders:
    state_name = os.path.basename(state_folder)
    output_path = f"{state_output_combined_path}/{state_name}"
    read_state_data(state_folder, output_path, state_name)

# Stop the Spark session
spark.stop()
