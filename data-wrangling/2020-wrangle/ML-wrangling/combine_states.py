from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import lit, col, concat
def combine_states(states):
    """reads the csvs for all states given and unions them all into a data frame and saves to orc"""
    dfs = [spark.read.csv(f"statecsvs/{st}.csv", header=True, inferSchema=True) for st in states]
    df = dfs[0]
    for i in range(1,len(dfs)):
        df = df.union(dfs[i])
    df = df.withColumn("Custom_Decade", lit(2020)).withColumn("Custom_Unique_Key",
                                        concat(lit("2020-"), col("STUSAB"), lit("-"), col("LOGRECNO")))
    df.coalesce(1).write.mode("overwrite").orc("2020part3.orc")

states = ["mt","nc","nd","ne","nh","nj","nm","nv","ny","oh","ok","or","pa"]
combine_states(states)