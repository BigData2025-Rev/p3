import spark_singleton as ss
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataLoader():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self):
        self.__data_list = []
        self.__columns = None
        self.__excluded = []


    @property
    def data(self):
        _data: DataFrame = None
        for df in self.__data_list:
            if _data is None:
                _data = df
                _data = _data.select(self.__columns)
            else:
                df = df.select(self.__columns)
                _data = _data.union(df)
        
        return _data

    def add_data_from(self, filename):
        df: DataFrame = DataLoader.spark.read.format("orc").load(filename)
        if self.__columns is None:
            self.__columns = set(df.columns)
        else:
            self.__columns = self.__columns & set(df.columns)
        self.__data_list.append(df)

    def set_excluded_columns(self):
        self.__excluded = sorted(list(set([column for df in self.__data_list for column in df.columns if column not in self.__columns])))
        self.__columns = list(self.__columns)
        print(self.__excluded)

    def stop(self):
        DataLoader.spark.stop()