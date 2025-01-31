from pyspark.sql import SparkSession
import 2010_wrangler_constant

#create Spark session
spark = SparkSession.builder\
    .appName("DataProcessing") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.shuffle.file.buffer", "64k") \
    .config("spark.shuffle.spill.compress", "true") \
    .config("spark.shuffle.compress", "true") \
    .getOrCreate()

combine_df = None
try:
    #loop through all states in the 2010_wrangler_constant file
    for state in constant.STATES:
        print(f"Combining {state}")
        df = spark.read.orc(f"final/2010/{state}/output")
        print(f"Add {df.count()}")
        # if combine_df isn't None add df to it
        if combine_df:
            combine_df = combine_df.union(df)
        else:
            combine_df = df
        print(f"Successfully combining {state}")
    print(f"TOTAL {combine_df.count()}")
    #write combine_df to file after looping through all state dataframes
    combine_df.write.mode("overwrite").orc("/final/2010/ALL")
except Exception as e:
    print(e)
    combine_df.write.mode("overwrite").orc("/final/2010/ALL")
