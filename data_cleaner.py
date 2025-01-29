from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

class DataCleaner():

    def __init__(self, data: DataFrame):
        self.__data = data

    @property
    def data(self) -> DataFrame:
        data: DataFrame = self.__data
        return data
    
    def using_total_population(self):
        """
            Example for individual methods that are responsible for a single task 
            like making sure total population is included, has the right data type, and it is named appropriately.

            Returns: A new DataCleaner object with the resulting DataFrame passed to its constructor.
        """
        data: DataFrame = self.__data
        data = data.withColumn('total_population', data['P0010001'].cast(IntegerType())).drop('P0010001')
        return DataCleaner(data)
    
    # def total_population_over_states(self):
    #     data: DataFrame = self.__data #doing this allows us to use intellisense to see the various dataframe transformations and actions.
    #     data = data[col('SUMLEV') == 40].select(['STUSAB', 'total_population']).groupBy('STUSAB').sum('total_population').withColumnRenamed('sum(total_population)', 'total_population')
    #     return DataCleaner(data)