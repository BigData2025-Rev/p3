import os
from dotenv import load_dotenv

load_dotenv('.env')
"""
    #WSL:
    #pip3 install python-dotenv
    Loads environment variables
    Set the environment variables in the .env file
    For example:
    SPARK_MASTER=local <--- Notice that there is no quotation marks

    Make sure .env file exists, and there are no spaces.
"""


SPARK_APP_NAME = 'Census Bureau Data Cleaner'
HDFS_MASTER = os.environ.get("HDFS_MASTER")
HDFS_USER = os.environ.get("HDFS_USER")
HDFS_FILENAME_1 = os.environ.get("HDFS_FILENAME_1")
HDFS_FILENAME_2 = os.environ.get("HDFS_FILENAME_2")
HDFS_FILENAME_3 = os.environ.get("HDFS_FILENAME_3")

HDFS_DATA_DIR_1 = f"{HDFS_MASTER}/user/{HDFS_USER}/{HDFS_FILENAME_1}"
HDFS_DATA_DIR_2 = f"{HDFS_MASTER}/user/{HDFS_USER}/{HDFS_FILENAME_2}"
HDFS_DATA_DIR_3 = f"{HDFS_MASTER}/user/{HDFS_USER}/{HDFS_FILENAME_3}"