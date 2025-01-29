from pyspark.sql import SparkSession
import constant

spark = SparkSession.builder\
    .appName("DataProcessing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.shuffle.file.buffer", "64k") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .getOrCreate()

geo_header_broadcast = spark.sparkContext.broadcast(constant.GEO_HEADER)
def process_line(line):
    geo_header = geo_header_broadcast.value
    return [eval(f"line[{v}]").strip() for k, v in geo_header.items()]

for state in constant.STATES:
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

    output = geo_df.join(p1_df, on="LOGRECNO", how="outer")
    output = output.join(p2_df, on="LOGRECNO", how="outer")
    output = output.sort("LOGRECNO", ascending=True)
    output = output.select(*constant.COMBINE_HEADER)
    output.write.mode("overwrite").orc(f"final/2010/{state}/output")
