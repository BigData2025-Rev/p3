import os
from pathlib import Path
from data_loader import DataLoader
from data_cleaner import DataCleaner
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from config import HDFS_DATA_DIR_1, HDFS_DATA_DIR_2, HDFS_DATA_DIR_3 #, HDFS_NATIONAL_DIR_1
# from pyspark.sql.functions import col
# from pyspark.sql.types import IntegerType
from logger import logger


def save_dataframe_to_localpath(data: DataFrame):
    #get absolute path to current working directory
    path = Path(__file__).parent / "clean_data.orc"

    #reduce partitions to one
    data: DataFrame = data.coalesce(1)

    #save to orc file
    data.write.mode('overwrite').format('orc').save(f"file://{str(path.absolute())}")
    
def main():
    logger.info("Pipeline execution started.")

    data_loader = DataLoader()

    data_loader.add_data_from(HDFS_DATA_DIR_1)
    data_loader.add_data_from(HDFS_DATA_DIR_2)
    data_loader.add_data_from(HDFS_DATA_DIR_3)
    data_loader.set_excluded_columns()

    # national_data = data_loader.load_from_file(HDFS_NATIONAL_DIR_1)
    # data_loader.debug_data(national_data)

    data: DataFrame = data_loader.data

    SUMMARY_LEVELS = [40, 50, 160, 500]

    FINAL_COLUMNS = ['unique_key', 'year', 'state_abbr', 'logrecno', 'summary_level', 'county', 'name', 'district', 'total_population', 'white_population', \
                     'black_population', 'american_indian_population', 'asian_population', 'native_hawaiian_population', 'other_race_population', \
                     'two_or_more_races_population', 'total_adult_pop', 'region', 'metro_status']
    

    cleaned_data: DataFrame = DataCleaner(data) \
                            .filter_summary_levels(SUMMARY_LEVELS) \
                            .using_total_population() \
                            .using_white_population() \
                            .using_black_population() \
                            .using_american_indian_population() \
                            .using_asian_population() \
                            .using_native_hawaiian_population() \
                            .using_other_race_population() \
                            .using_two_or_more_races_population() \
                            .using_region() \
                            .add_year() \
                            .add_geodata() \
                            .using_composite_key() \
                            .using_total_population_adult() \
                            .select_data(FINAL_COLUMNS) \
                            .data

    #Output to ORC
    save_dataframe_to_localpath(cleaned_data)
    logger.info("Pipeline execution completed successfully.")
    cleaned_data.show()
    cleaned_data.printSchema()

    data_loader.stop()



if __name__ == '__main__':
    main()