from data_loader import DataLoader
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from config import HDFS_DATA_DIR_1

def main():
    data_loader = DataLoader(HDFS_DATA_DIR_1)
    data: DataFrame = data_loader.data

    data[col('SUMLEV') == 50].show()

    data_loader.stop()



if __name__ == '__main__':
    main()