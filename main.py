from data_loader import DataLoader
from data_cleaner import DataCleaner
from pyspark.sql import DataFrame
from config import HDFS_DATA_DIR_1

def main():
    data_loader = DataLoader(HDFS_DATA_DIR_1)
    data: DataFrame = data_loader.data

    SUMMARY_LEVELS = [50, 500, 160, 40]

    FINAL_COLUMNS = ['year', 'state_abbr', 'logrecno', 'summary_level', 'county', 'city_name', 'district', 'total_population', 'white_population', 'black_population', \
                     'american_indian_population', 'asian_population', 'native_hawaiian_population', 'other_race_population', 'two_or_more_races_population']

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
                            .add_year() \
                            .add_geodata(2000) \
                            .select_data(FINAL_COLUMNS) \
                            .data

    #Ouptut to CSV
    cleaned_data.coalesce(1).write.csv("2000_combined_states_filtered.csv", header=True, mode="overwrite")

    #Output to ORC
    cleaned_data.write.orc("2000_combined_states_filtered.orc", mode="overwrite")
    
    cleaned_data.printSchema()

    data_loader.stop()



if __name__ == '__main__':
    main()