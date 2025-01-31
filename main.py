from data_loader import DataLoader
from data_cleaner import DataCleaner
from pyspark.sql import DataFrame
from config import HDFS_DATA_DIR_1, HDFS_DATA_DIR_2, HDFS_DATA_DIR_3 #, HDFS_NATIONAL_DIR_1
# from pyspark.sql.functions import col
# from pyspark.sql.types import IntegerType

def main():
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
                     'two_or_more_races_population', 'total_adult_pop', 'region', 'urban_rural', 'metro_status']
    

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
                            .using_urban_rural() \
                            .using_region() \
                            .add_year() \
                            .add_geodata() \
                            .using_composite_key() \
                            .using_total_adult_population() \
                            .select_data(FINAL_COLUMNS) \
                            .data

    #Ouptut to CSV
    #cleaned_data.coalesce(1).write.csv("final_data.csv", header=True, mode="overwrite")

    #Output to ORC
    # cleaned_data.write.orc("cleaned_data.orc", mode="overwrite")
    cleaned_data.show()
    # cleaned_data.filter(col('summary_level') == 500).select(['state_abbr', 'total_population', 'urban_rural']).show()
    cleaned_data.printSchema()

    data_loader.stop()



if __name__ == '__main__':
    main()