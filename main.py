import csv
from pyspark.sql import SparkSession
import constant

spark = SparkSession.builder.getOrCreate()

def process_line(line):
    line_info = []
    for k, v in constant.GEO_HEADER.items():
        line_info.append(eval(f"line[{v}]").strip())
    return line_info

for state in constant.STATES:
    # print(state)

    geo_file = f"final/2010/{state}/*geo2010.pl"
    p1_file = f"final/2010/{state}/*000012010.pl"
    p2_file = f"final/2010/{state}/*000022010.pl"

    geo_rdd = spark.sparkContext.textFile(geo_file)
    geo_rdd = geo_rdd.map(process_line)
    geo_df = geo_rdd.toDF(list(constant.GEO_HEADER.keys()))

    p1_df = spark.read.csv(p1_file)
    p1_df = p1_df.toDF(*constant.P1_HEADER)
    p1_df = p1_df.drop("FILEID","STUSAB","CHARITER","CIFSN")

    p2_df = spark.read.csv(p2_file)
    p2_df = p2_df.toDF(*constant.P2_HEADER)
    p2_df = p2_df.drop("FILEID","STUSAB","CHARITER","CIFSN")

    output = geo_df.join(p1_df, on="LOGRECNO", how="outer")\
                    .join(p2_df, on="LOGRECNO", how="outer")
    output = output.sort("LOGRECNO", ascending=True)
    output = output.select(*constant.COMBINE_HEADER)
    output.coalesce(1).write.mode("overwrite").orc(f"final/2010/{state}/output")
