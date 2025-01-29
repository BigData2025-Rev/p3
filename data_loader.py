import spark_singleton as ss
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

class DataLoader():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self):
        self.__data_list = []
        self.__columns = None
        self.__excluded = []

    def get_year(self, index):
        year_list = [2000, 2010, 2020]
        return year_list[index]

    def get_select_columns(self, index):
        cd_difference = ['CD106', 'CD', 'CD116']

        select_columns = self.__columns.copy()
        select_columns.append(cd_difference[index])
        return select_columns
    
    @property
    def data(self):
        _data: DataFrame = None
        
        
        for index, df in enumerate(self.__data_list):
            if _data is None:
                _data: DataFrame = df
                
                _data: DataFrame = _data.select(self.get_select_columns(index))
                _data: DataFrame = _data.withColumn('Custom_Decade', lit(self.get_year(index)))
            else:
                select_columns = self.__columns.copy()
                df: DataFrame = df.select(self.get_select_columns(index))
                df: DataFrame = df.withColumn('Custom_Decade', lit(self.get_year(index)))
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