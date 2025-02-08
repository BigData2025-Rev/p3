import spark_singleton as ss
from pyspark.sql import DataFrame

class DataLoader():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self, filename):
        self.__data = self.load_from_file(filename)

    @property
    def data(self):
        return self.__data

    def load_from_file(self, filename):
        df: DataFrame = DataLoader.spark.read.format("orc").load(filename)
        return df
            
    def stop(self):
        DataLoader.spark.stop()