import re
import spark_singleton as ss
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat, udf
from pyspark.sql.types import StringType, IntegerType
from logger import logger

@udf(StringType())
def extract_logrecno(entry):
    matched = re.search(r'\d+', entry) 
    return matched.group()

class DataLoader():
    spark = ss.SparkSingleton.getInstance()

    def __init__(self):
        self.__data_list = []
        self.__columns = None
        self.__excluded = []

    def get_year(self, index):
        year_list = [2000, 2010, 2020]
        return year_list[index]

    def get_select_columns(self, index: int):
        cd_difference = ['CD106', 'CD', 'CD116']
        pci_difference = ['MACCI', 'CBSAPCI', 'CBSAPCI']

        select_columns = self.__columns.copy()
        select_columns.append(cd_difference[index])
        select_columns.append(pci_difference[index])
        return select_columns
    
    def select_columns(self, data: DataFrame, select_columns: list[str]):
        data = data.select(select_columns)
        return data
    
    def rename_select_columns(self, data: DataFrame, select_columns: list[str]):
        data: DataFrame = data.withColumnRenamed(select_columns[-2], 'district')
        data: DataFrame = data.withColumnRenamed(select_columns[-1], 'metro_status')
        return data
    
    def add_decade(self, data: DataFrame, index):
        data = data.withColumn('Custom_Decade', lit(self.get_year(index)))
        return data
    
    def extract_logrecno(self, data: DataFrame):
        data: DataFrame = data.withColumn('LOGRECNO', extract_logrecno('LOGRECNO'))
        return data

    def add_composite_key(self, data: DataFrame):
        data = data.withColumn('Custom_Unique_Key', concat(col('Custom_Decade'),lit('-'), col('STUSAB'),lit('-'), col('LOGRECNO')))
        
        return data
    

    @property
    def data(self):
        self.__data = None        
        for index, df in enumerate(self.__data_list):
            if self.__data is None:
                self.__data = df
                select_columns = self.get_select_columns(index)
                self.__data = self.select_columns(self.__data, select_columns)
                self.__data = self.rename_select_columns(self.__data, select_columns)
                self.__data = self.add_decade(self.__data, index)
                self.__data = self.extract_logrecno(self.__data)
                self.__data = self.add_composite_key(self.__data)
            else:
                select_columns = self.get_select_columns(index)
                df: DataFrame = self.select_columns(df, select_columns)
                df: DataFrame = self.rename_select_columns(df, select_columns)
                df: DataFrame = self.add_decade(df, index)
                df: DataFrame = self.extract_logrecno(df)
                df: DataFrame = self.add_composite_key(df)
                self.__data = self.__data.union(df)

        logger.info(f"Final combined dataset has {self.__data.count()} rows and {len(self.__data.columns)} columns.")
        return self.__data

    def add_data_from(self, filename):
        logger.info(f"Loading data from {filename}")
        try:
            df: DataFrame = DataLoader.spark.read.format("orc").load(filename)
            logger.info(f"Successfully loaded {df.count()} rows and {len(df.columns)} columns from {filename}")
            if self.__columns is None:
                self.__columns = set(df.columns)
            else:
                self.__columns = self.__columns & set(df.columns)
            self.__data_list.append(df)
        except Exception as e:
            logger.error(f"Failed to load data from {filename}: {str(e)}") 

    def set_excluded_columns(self):
        self.__excluded = sorted(list(set([column for df in self.__data_list for column in df.columns if column not in self.__columns])))
        self.__columns = list(self.__columns)

    def load_from_file(self, filename):
        df: DataFrame = DataLoader.spark.read.format("orc").load(filename)
        return df
    def debug_data(self, data: DataFrame):
        data = data.withColumn('SUMLEV', col('SUMLEV').cast(IntegerType()))
        summary_levels = data.select('SUMLEV').distinct().collect()
        for summary_level in summary_levels:
            data.filter(col('SUMLEV') == summary_level['SUMLEV']).select(['STUSAB', 'LOGRECNO','GEOCOMP', 'UR']).show()
            tst = input(f"Current: {summary_level} \nPress any key to continue...")
            
    def stop(self):
        DataLoader.spark.stop()