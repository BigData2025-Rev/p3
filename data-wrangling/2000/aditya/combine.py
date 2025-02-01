from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat
from pyspark import StorageLevel
import pandas as pd
import os

#.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
spark = SparkSession.builder.appName('census-data-collector')\
                            .config("spark.driver.memory", "16g")\
                            .config("spark.executor.memory", "4g")\
                            .config("spark.default.parallelism", 400)\
                            .config("spark.executor.instances", "200")\
                            .config("spark.sql.shuffle.partitions", "200")\
                            .config("spark.executor.cores", "4")\
                            .getOrCreate()
base = "output"
#base = "redistricting_file--pl_94-171"
master_df = None

#def create_state_file(val):    
    #combined_path = os.path.join(base , state_dir)
    
    #if os.path.isdir(combined_path):
    #    print(combined_path)
    #    first, second, geo = os.listdir(combined_path)
    #    first_df = spark.read.csv(os.path.join(combined_path,first), header=True, inferSchema=True)
        #second_df = spark.read.csv(os.path.join(combined_path,second), header=True, inferSchema=True)
        #geo_df = spark.read.csv(os.path.join(combined_path,geo), header=True, inferSchema=True)
        #common_columns = set(first_df.columns).intersection(set(second_df.columns)).intersection(set(geo_df.columns))
        #for col_name in common_columns:
        #    first_df = first_df.withColumnRenamed(col_name, col_name +"_df1")
        #    second_df = second_df.withColumnRenamed(col_name, col_name +"_df2")
        #first_df = first_df.withColumnRenamed('LOGRECNO', 'LOGRECNO_df1')
        #merged_df = geo_df.join(first_df, on='LOGRECNO', how='outer')
        #merged_df = merged_df.join(second_df, on='LOGRECNO', how='outer')
        #for col_name in common_columns:
        #    merged_df = merged_df.drop(col_name + "_df1")
        #    merged_df = merged_df.drop(col_name + "_df2")
        #merged_df = merged_df.withColumn("id", concat(col('LOGRECNO'), col('STUSAB')))
    #    return first_df

directories = os.listdir(base)
state_dir = []
unioned_df = None
#print(directories)
for state in directories:
    print(state)
    combined_path = os.path.abspath(os.path.join(base , state))
    #print(state, base, os.path.abspath(os.path.join(base , state)))
    #print(os.listdir(combined_path))
    first, second, geo = os.listdir(combined_path)
    first_df = spark.read\
                        .option("spark.sql.files.maxPartitionBytes", "10485760")\
                        .csv(f"file://{os.path.join(combined_path,first)}", header=True, inferSchema=False).repartition(200)
    second_df = spark.read\
                    .option("spark.sql.files.maxPartitionBytes", "10485760")\
                    .csv(f"file://{os.path.join(combined_path,second)}", header=True, inferSchema=False).repartition(200)
    geo_df = spark.read\
                    .option("spark.sql.files.maxPartitionBytes", "10485760")\
                    .csv(f"file://{os.path.join(combined_path,geo)}", header=True, inferSchema=False).repartition(200)
    
    # drop columns common among all three dataframes
    set_1 =  set(first_df.columns)
    set_2 = set(second_df.columns)
    set_geo = set(geo_df.columns)
    common_columns = set_1.intersection(set_2).intersection(set_geo)
    common_columns.remove('LOGRECNO')

    first_df = first_df.drop(*common_columns)
    second_df = second_df.drop(*common_columns)

    #print(first_df.columns, second_df.columns, geo_df.columns)
    merged_df = geo_df.join(first_df, on='LOGRECNO', how='outer')
    merged_df = merged_df.join(second_df, on='LOGRECNO', how='outer')
    merged_df.withColumn("STATE-LOGREC", concat(col('STUSAB'), col('LOGRECNO'))) 
    merged_df = merged_df.persist(StorageLevel.DISK_ONLY)
    #print(merged_df.columns)

    merged_df = merged_df.coalesce(1)
    merged_df.write.option("header", "true").csv(f"/user/spark/states/{state}")
    first_df.unpersist()
    second_df.unpersist()
    geo_df.unpersist()

    #print(merged_df.columns)

    # union approach bricks the system.
    '''if not unioned_df:
        unioned_df = merged_df
        unioned_df.repartition(100)
        unioned_df.persist(StorageLevel.DISK_ONLY)
    else:
        unioned_df = unioned_df.unionByName(merged_df).persist(StorageLevel.DISK_ONLY)
        unioned_df.repartition(100)
    
    column_dict = {}
    for name in unioned_df.columns:
        if name in column_dict:
            column_dict[name] += 1
        else:
            column_dict[name] = 1
    #print(column_dict)
    merged_df.unpersist()
    unioned_df.unpersist()
    '''
#sc = spark.sparkContext
#rdd = sc.parallelize(state_dir)
#states_rdd = rdd.map(create_state_file)

#dataframes = states_rdd.collect()
#combined = dataframes[0]
#for df in dataframes[1:]:
#    combined = combined.union(df)

#unioned_df = unioned_df.coalesce(1)
#unioned_df.write.option("header", "true").csv("/user/spark/project3")
combined_df = spark.read.csv("/user/spark/states")
combined_df.coalesce(1).write.option("header", "true").csv("/user/spark/country")
spark.stop()
