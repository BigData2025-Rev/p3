from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType
from logger import logger

class DataCleaner():

    def __init__(self, data: DataFrame):
        self.__data = data

    @property
    def data(self) -> DataFrame:
        data: DataFrame = self.__data
        return data

    #Filters summary levels, pass in a list of summary levels to filter by.
    def filter_summary_levels(self, summary_levels: list[int]):
        """
            Filters the DataFrame to include only rows with specified summary levels.
            
            Args:
                summary_levels (list[int]): A list of summary levels (SUMLEV) to include in the filtered DataFrame.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the filtered DataFrame.
        """
        logger.info(f"Filtering dataset to keep only summary levels: {summary_levels}")
        try:
            data: DataFrame = self.__data
            before_count = data.count()
            data = data.filter(col('SUMLEV').isin(summary_levels))
            after_count = data.count()
            logger.info(f"Filtered dataset from {before_count} to {after_count} rows.")
            return DataCleaner(data)
        except Exception as e:
            logger.error(f"Error filtering summary levels: {str(e)}")

    def using_total_population(self):
        """
            Renames the 'P0010001' column to 'total_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        logger.info("Renaming and casting 'P0010001' to 'total_population' as IntegerType.")
        try:
            data: DataFrame = self.__data
            data = data.withColumnRenamed('P0010001', 'total_population')
            data = data.withColumn('total_population', col('total_population').cast(IntegerType()))
            return DataCleaner(data)
        except Exception as e: 
            logger.error(f"Error in using_total_population: {str(e)}")
            
    #Methods for demographic data
    def using_white_population(self):
        """
            Renames the 'P0010003' column to 'white_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010003', 'white_population')
        data = data.withColumn('white_population', col('white_population').cast(IntegerType()))
        return DataCleaner(data)
    
    def using_black_population(self):
        """
            Renames the 'P0010004' column to 'black_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010004', 'black_population')
        data = data.withColumn('black_population', col('black_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_american_indian_population(self):
        """
            Renames the 'P0010005' column to 'american_indian_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010005', 'american_indian_population')
        data = data.withColumn('american_indian_population', col('american_indian_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_asian_population(self):
        """
            Renames the 'P0010006' column to 'asian_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010006', 'asian_population')
        data = data.withColumn('asian_population', col('asian_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_native_hawaiian_population(self):
        """
            Renames the 'P0010007' column to 'native_hawaiian_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010007', 'native_hawaiian_population')
        data = data.withColumn('native_hawaiian_population', col('native_hawaiian_population').cast(IntegerType()))
        return DataCleaner(data)


    def using_other_race_population(self):
        """
            Renames the 'P0010008' column to 'other_race_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010008', 'other_race_population')
        data = data.withColumn('other_race_population', col('other_race_population').cast(IntegerType()))
        return DataCleaner(data)

    def using_two_or_more_races_population(self):
        """
            Renames the 'P0010009' column to 'two_or_more_races_population' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0010009', 'two_or_more_races_population')
        data = data.withColumn('two_or_more_races_population', col('two_or_more_races_population').cast(IntegerType()))
        return DataCleaner(data)

    #Adds the year column to the dataframe.
    def add_year(self):
        """
            Renames the 'Custom_Decade' column to 'year' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('Custom_Decade', 'year')
        data = data.withColumn('year', col('year').cast(IntegerType()))
        return DataCleaner(data)

    #Adds data from the geodata portion of the data and other needed columns.
    def add_geodata(self):
        """
            Renames columns related to geographic data (e.g., state abbreviation, county, city name, etc.).
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('STUSAB', 'state_abbr')
        data = data.withColumnRenamed('COUNTY', 'county')
        data = data.withColumnRenamed('LOGRECNO', 'logrecno')
        data = data.withColumnRenamed('NAME', 'name')
        data = data.withColumnRenamed('SUMLEV', 'summary_level')
        return DataCleaner(data)

    #Selects the columns we need for the final output. Pass in a list of columns to select.
    def select_data(self, columns: list[str]):
        """
            Selects specific columns from the DataFrame and returns a new DataCleaner instance with the subset of data.

            Args:
                columns (list[str]): A list of column names to include in the resulting DataFrame.

            Returns:
                DataCleaner: A new DataCleaner instance containing only the specified columns.
        """
        data: DataFrame = self.__data
        data = data.select(columns)
        return DataCleaner(data)

    def add_metro_status(self):
        """
            Categorizes areas into 'Metropolitan' or 'Non-Metropolitan' based on the MACCI column and leaves null values as null.
            - 'Y' → 'Metropolitan'
            - 'N' or '9' → 'Non-Metropolitan'

            Returns:
                DataCleaner: A new DataCleaner instance containing only the specified columns.
        """
        data: DataFrame = self.__data
        # data = data.withColumnRenamed("MACCI", "metro_status")
        data = data.withColumn(
            "metro_status",
            when(col("metro_status") == "Y", "Metropolitan")
            .when(col("metro_status").isNull(), None)
            .otherwise("Non-Metropolitan")
        )
        return DataCleaner(data)
        

    def using_region(self):
        """
            Categorize areas into region based on the REGION column and leaves null values as null.
            1 == Northeast,
            2 == Midwest,
            3 == South,
            4 == West,
            9 == Not in mainland

            Returns:
                DataCleaner: A new DataCleaner instance containing only the specified columns.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed("REGION", "region")
        data = data.withColumn('region', when(col("region") == '1', 'Northeast') \
                                .when(col("region") == '2', 'Midwest') \
                                .when(col("region") == '3', 'South') \
                                .when(col("region") == '4', 'West') \
                                .when(col("region").isNull(), None)
                                .otherwise('Other'))
        return DataCleaner(data)
    
    # def using_urban_rural(self):
    #     """
    #         Add Urban or Rural classification column and leaves null values as null.
    #         - U -> wholly urban
    #         - R -> wholly rural
    #         - M -> Mixed

    #         Returns:
    #             DataCleaner: A new DataCleaner instance containing only the specified columns.
    #     """
    #     data: DataFrame = self.__data
    #     data = data.withColumnRenamed("UR", "urban_rural")
    #     data = data.withColumn('urban_rural', when(col("urban_rural") == 'U', 'Urban') \
    #                             .when(col("urban_rural") == 'R', 'Rural') \
    #                             .when(col("urban_rural").isNull(), None) \
    #                             .otherwise('Mixed'))
    #     return DataCleaner(data)
    
    def using_composite_key(self):
        data: DataFrame = self.__data
        data = data.withColumnRenamed('Custom_Unique_Key','unique_key')
        return DataCleaner(data)

    def using_total_population_adult(self):
        """
            Renames the 'P0030001' column to 'total_adult_pop' and casts it to an integer type.
            
            Returns:
                DataCleaner: A new DataCleaner instance with the updated DataFrame.
        """
        data: DataFrame = self.__data
        data = data.withColumnRenamed('P0030001', 'total_adult_pop')
        data = data.withColumn('total_adult_pop', col('total_adult_pop').cast(IntegerType()))
        return DataCleaner(data)