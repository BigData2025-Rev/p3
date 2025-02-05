from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, pandas_udf, lit, lag, when, bround
from pyspark.sql.window import Window
import pandas as pd


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
    
    @staticmethod
    @pandas_udf(returnType=IntegerType())
    def make_prediction(independent: pd.Series, dependent: pd.Series) -> int:
        """
            Params: Independent and Dependent Column
            Returns: Predicted value assuming linear relationship.
        """
        TARGET_YEAR = 2030
        n = independent.count()

        x_sum = independent.sum()
        y_sum = dependent.sum()
        x_squared_sum = (independent * independent).sum()
        xy_sum = (independent * dependent).sum()

        intercept = (y_sum*x_squared_sum - x_sum * xy_sum) / (n * x_squared_sum - (x_sum ** 2))
        slope = (n * xy_sum - x_sum * y_sum) / (n * x_squared_sum - (x_sum ** 2))
        prediction = intercept + slope * TARGET_YEAR

        return round(prediction)
                
    def using_future_population(self):
        data: DataFrame = self.__data
        data = data.withColumn('id', lit(1))
        data = data.groupBy('id') \
                    .agg(self.make_prediction(col('year'), col('total_population'))) \
                    .withColumnRenamed('make_prediction(year, total_population)','total_population')
        
        return NationalTotalAnalysis(data)
    
    def add_year(self, year: int):
        data: DataFrame = self.__data
        data = data.withColumn('year', lit(year))

        if 'id' in data.columns:
            data = data.drop('id')
        
        return NationalTotalAnalysis(data)
    
    def aggregate_population_by_year(self):
        data: DataFrame = self.__data
        data = data.groupBy('year') \
                    .agg({'total_population':'sum'}) \
                    .withColumnRenamed('sum(total_population)', 'total_population') \
                    .sort('year')
        
        return NationalTotalAnalysis(data)
    
    def using_percent_growth(self):
        data: DataFrame = self.__data
        population_window = Window.orderBy('year')
        data = data.withColumn('prev_population', lag('total_population').over(population_window)) \
                    .withColumn('percent_growth',  
                                when(col('prev_population').isNotNull(),
                                     (col('total_population') - col('prev_population')) / col('prev_population') * 100) \
                                .otherwise(None)) \
                    .withColumn('percent_growth', when(col('percent_growth').isNotNull(),
                                                       bround('percent_growth', 2)) \
                                                    .otherwise(None))
        
        return NationalTotalAnalysis(data)

    