from data_loader import DataLoader
from data_cleaner import DataCleaner
from pyspark.sql import DataFrame
from config import HDFS_DATA_DIR_1

def main():
    data_loader = DataLoader(HDFS_DATA_DIR_1)
    data: DataFrame = data_loader.data

    cleaned_data: DataFrame = DataCleaner(data) \
                            .test_method() \
                            .data
    
    cleaned_data.show()

    data_loader.stop()



if __name__ == '__main__':
    main()