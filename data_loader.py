import spark_singleton as ss
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataLoader():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, filename):
        self.__data = DataLoader.spark.read.format("orc").load(filename)

    @property
    def data(self):
        return self.__data

    def stop(self):
        DataLoader.spark.stop()