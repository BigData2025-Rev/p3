from pyspark.sql import DataFrame
from pyspark.sql.functions import col

class DataCleaner():

    def __init__(self, data: DataFrame):
        self.__data = data

    @property
    def data(self) -> DataFrame:
        data: DataFrame = self.__data
        return data
    
    def test_method(self) -> DataFrame:
        data: DataFrame = self.__data #doing this allows us to use intellisense to see the various dataframe transformations and actions.
        data = data[col('SUMLEV') == 50].select(['STUSAB', 'P0010001'])
        return DataCleaner(data)