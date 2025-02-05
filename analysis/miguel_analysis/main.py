import os
from pathlib import Path
from pyspark.sql import DataFrame
from data_loader import DataLoader
from national_total_analysis import NationalTotalAnalysis
from config import HDFS_DATA_DIR


def save_dataframe_to_localpath(data: DataFrame, filename: str):
    #get absolute path to current working directory
    path = Path(__file__).parent / filename

    #reduce partitions to one
    data: DataFrame = data.coalesce(1)

    #save to orc file
    data.write.mode('overwrite').format('orc').save(f"file://{str(path.absolute())}")


def main():

    data_loader = DataLoader(HDFS_DATA_DIR)
    clean_data: DataFrame = data_loader.data
    # clean_data.printSchema()

    result: DataFrame = NationalTotalAnalysis(clean_data) \
                        .get_state_level_data() \
                        .aggregate_population_by_year() \
                        .data
    
    
    
    future_result: DataFrame = NationalTotalAnalysis(clean_data) \
                        .get_state_level_data() \
                        .aggregate_population_by_year() \
                        .using_future_population() \
                        .add_year(2030) \
                        .data
    
    
    # NationalTotalAnalysis(clean_data) \
    #                     .get_trend_line()
    result = result.union(future_result.select(['year', 'total_population']))
    percent_change: DataFrame = NationalTotalAnalysis(result) \
                        .using_percent_growth() \
                        .data
    # result.show()
    percent_change.show()

    save_dataframe_to_localpath(percent_change, 'analysis_result.orc')

    # result.printSchema()
    data_loader.stop()

if __name__ == '__main__':
    main()