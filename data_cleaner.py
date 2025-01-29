from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
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
            Author: Miguel + ideas from team.
            Example for individual methods that are responsible for a single task 
            like making sure total population is included, has the right data type, and it is named appropriately.

            Returns: A new DataCleaner object with the resulting DataFrame passed to its constructor.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010001', 'total_population')
        data = data.withColumn('total_population', col('total_population').cast(IntegerType()))
        return DataCleaner(data)

    #Filters summary levels, pass in a list of summary levels to filter by.
    def filter_summary_levels(self, summary_levels: list[int]):
        data: DataFrame = self.__data
        data = data.filter(col('SUMLEV').isin(summary_levels))
        return DataCleaner(data)

    def using_total_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010001', 'total_population')
        data = data.withColumn('total_population', col('total_population').cast(IntegerType()))
        return DataCleaner(data)
    
    #Methods for demographic data
    def using_white_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010003', 'white_population')
        data = data.withColumn('white_population', col('white_population').cast(IntegerType()))
        return DataCleaner(data)
    
    def using_black_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010004', 'black_population')
        data = data.withColumn('black_population', col('black_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_american_indian_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010005', 'american_indian_population')
        data = data.withColumn('american_indian_population', col('american_indian_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_asian_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010006', 'asian_population')
        data = data.withColumn('asian_population', col('asian_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_native_hawaiian_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010007', 'native_hawaiian_population')
        data = data.withColumn('native_hawaiian_population', col('native_hawaiian_population').cast(IntegerType()))
        return DataCleaner(data)


    def using_other_race_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010008', 'other_race_population')
        data = data.withColumn('other_race_population', col('other_race_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_two_or_more_races_population(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010009', 'two_or_more_races_population')
        data = data.withColumn('two_or_more_races_population', col('two_or_more_races_population').cast(IntegerType()))
        return DataCleaner(data)

    #Adds the year column to the dataframe.
    #Will need to change this for each decade if other groups do not use the same column name.
    def add_year(self) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('Custom_Decade', 'year')
        data = data.withColumn('year', col('year').cast(IntegerType()))
        return DataCleaner(data)

    #Adds data from the geodata portion of the data and other needed columns.
    def add_geodata(self, decade: int) -> DataFrame:
        data: DataFrame = self.__data
        data = data.withColumnRenamed('STUSAB', 'state_abbr')
        data = data.withColumnRenamed('COUNTY', 'county')
        data = data.withColumnRenamed('LOGRECNO', 'logrecno')
        data = data.withColumnRenamed('NAME', 'city_name')
        data = data.withColumnRenamed('SUMLEV', 'summary_level')

        #2000: CD106, 2010: CD, 2020: CD116 (double check these are correct)
        #Now handled at the DataLoader class.
        # if decade == 2000:
        #     data = data.withColumnRenamed('CD106', 'district')
        # elif decade == 2010:
        #     data = data.withColumnRenamed('CD', 'district')
        # elif decade == 2020:
        #     data = data.withColumnRenamed('CD116', 'district')
        return DataCleaner(data)

    #Selects the columns we need for the final output. Pass in a list of columns to select.
    def select_data(self, columns: list[str]) -> DataFrame:
        data: DataFrame = self.__data
        data = data.select(columns)
        return DataCleaner(data)

    def categorize_metro_status(self):
        """
        Categorizes areas into 'Metropolitan' or 'Non-Metropolitan' based on the MACCI column.
        - 'Y' → 'Metropolitan'
        - 'N' or '9' → 'Non-Metropolitan'
        """
        data: DataFrame = self.__data
        data = data.withColumn(
            "Metro_Status",
            when(col("MACCI") == "Y", "Metropolitan")
            .otherwise("Non-Metropolitan")
        )
        return DataCleaner(data)
    
    def using_region(self):
        data: DataFrame = self.__data
        data = data.withColumn('region', col('region').cast(IntegerType()))
        data = data.withColumn('region', when(data.region == 1, 'Northeast') \
                                .when(data.region == 2, 'Midwest') \
                                .when(data.region == 3, 'South') \
                                .when(data.region == 4, 'West') \
                                .otherwise('Other'))
        return DataCleaner(data)
    
    def using_urban_rural(self):
        data: DataFrame = self.__data
        data = data.withColumn('UR', col('UR').cast(IntegerType()))
        data = data.withColumn('UR', when(data.UR == 1, 'Urban') \
                                .when(data.UR == 2, 'Rural') \
                                .otherwise('Mixed'))
        return DataCleaner(data)