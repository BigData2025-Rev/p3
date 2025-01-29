from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

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

    #Just have everyone add the summary levels they need to a list and pass it in.
    def filter_summary_levels(self, summary_levels: list[int]) -> DataFrame:
        data: DataFrame = self.__data
        data = data[col('SUMLEV').isin(summary_levels)]
        return DataCleaner(data)

    #Change data type to int
    def select_total_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010001', 'total_population')
        data = data.withColumn('total_population', col('total_population').cast('int'))
        return DataCleaner(data)
    
    
    def select_white_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010003', 'white_population')
        data = data.withColumn('white_population', col('white_population').cast('int'))
        return DataCleaner(data)
    
    def select_black_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010004', 'black_population')
        data = data.withColumn('black_population', col('black_population').cast('int'))
        return DataCleaner(data)

    def select_american_indian_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010005', 'american_indian_population')
        data = data.withColumn('american_indian_population', col('american_indian_population').cast('int'))
        return DataCleaner(data)

    def select_asian_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010006', 'asian_population')
        data = data.withColumn('asian_population', col('asian_population').cast('int'))
        return DataCleaner(data)

    def select_native_hawaiian_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010007', 'native_hawaiian_population')
        data = data.withColumn('native_hawaiian_population', col('native_hawaiian_population').cast('int'))
        return DataCleaner(data)


    def select_other_race_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010008', 'other_race_population')
        data = data.withColumn('other_race_population', col('other_race_population').cast('int'))
        return DataCleaner(data)

    def select_two_or_more_races_population(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010009', 'two_or_more_races_population')
        data = data.withColumn('two_or_more_races_population', col('two_or_more_races_population').cast('int'))
        return DataCleaner(data)

    #Check what other decades called their year column
    def add_year(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('Custom_Decade', 'year')
        data = data.withColumn('year', col('year').cast('int'))
        return DataCleaner(data)

    #Which geodata included depends on summary levels we need.
    def add_geodata(self, decade: int) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('STUSAB', 'state_abbr')
        data = data.withColumnRenamed('COUNTY', 'county')
        data = data.withColumnRenamed('LOGRECNO', 'id')
        data = data.withColumnRenamed('NAME', 'city_name')

        #2000: CD106, 2010: CD110, 2020: CD116 (double check these are correct)
        if decade == 2000:
            data = data.withColumnRenamed('CD106', 'district')
        elif decade == 2010:
            data = data.withColumnRenamed('CD110', 'district')
        elif decade == 2020:
            data = data.withColumnRenamed('CD116', 'district')
        return DataCleaner(data)

    def select_data(self, columns: list[str]) -> DataFrame:
        data: DataFrame = self.__data
        data = data.select(columns)
        return DataCleaner(data)
