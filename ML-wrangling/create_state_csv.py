from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def combine2020state(st):
    '''given the state abbreviation, combines the data for a state into a single csv
    the header data for 2020 and the pl files for each state are required'''

    h = spark.read.csv("2020_header_FieldNames.csv", header=True, inferSchema=True)
    h1 = spark.read.csv("2020_segment1_FieldNames.csv", header=True, inferSchema=True)
    h2 = spark.read.csv("2020_segment2_FieldNames.csv", header=True, inferSchema=True)

    df = spark.read.schema(h.schema).csv(f"{st}2020.pl/{st}geo2020.pl",sep="|")

    df1 = spark.read.schema(h1.schema).csv(f"{st}2020.pl/{st}000012020.pl",sep="|")

    df2 = spark.read.schema(h2.schema).csv(f"{st}2020.pl/{st}000022020.pl",sep="|")

    df1 = df1.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    df2 = df2.drop("FILEID", "STUSAB", "CHARITER", "CIFSN")
    statedf = df.join(df1, on="LOGRECNO", how="outer") \
                        .join(df2, on="LOGRECNO", how="outer")
    statedf.coalesce(1).write.csv(f"{st}2020.csv", header=True, mode="overwrite")

for st in ["nd","ne","nh","nj","nm","nv","ny","oh","ok","or","pa"]:
    combine2020state(st)