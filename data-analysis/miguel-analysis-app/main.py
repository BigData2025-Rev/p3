from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from data_loader import DataLoader
from national_total_analysis import NationalTotalAnalysis
from config import HDFS_DATA_DIR

def main():

    data_loader = DataLoader(HDFS_DATA_DIR)
    clean_data: DataFrame = data_loader.data
    clean_data.printSchema()

    result: DataFrame = NationalTotalAnalysis(clean_data) \
                        .get_state_level_data() \
                        .aggregate_population_by_year() \
                        .data
    
    result.show()
    result.printSchema()
    data_loader.stop()

if __name__ == '__main__':
    main()