from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class NationalTotalAnalysis():
    def __init__(self, data: DataFrame):
        self.__data = data
    
    @property
    def data(self):
        return self.__data
    
    def get_state_level_data(self):
        data: DataFrame = self.__data
        data = data.filter(col('summary_level') == 40)
        return NationalTotalAnalysis(data)
    
    def aggregate_population_by_year(self):
        data: DataFrame = self.__data
        data = data.groupBy('year') \
                    .agg({'total_population':'sum'}) \
                    .withColumnRenamed('sum(total_population)', 'total_population')
        return NationalTotalAnalysis(data)

    