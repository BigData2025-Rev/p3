import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collections import defaultdict
import csv
from concurrent.futures import ThreadPoolExecutor
from ftplib import FTP, error_perm
import threading

from access_parser import AccessParser
from tqdm import tqdm
from pyspark.sql import SparkSession

from rui.unZipAllFiles import batch_process

HOST = "ftp.census.gov"
REMOTE_RESOURCE = "/census_2000/datasets/redistricting_file--pl_94-171"
LOCAL_PATH = "../../../redistricting_file--pl_94-171"
ACCESS_FILE = os.path.join(LOCAL_PATH, "0File_Structure/Access2000/PL2000_Access2000.mdb")


class DownloadUsingThreads:
    def __init__(self, host=""):
        self.ftp_host = host

    def download_dirs(self, remote_dir, local_dir):
        """
        TODO: Use this function to log in
              list the contents if directory
              and then use the threads to pick
              a connection from the queue.
        """


class DownloadFromHostNew:
    def __init__(self, host=""):
        self.ftp_host = host
        self.spark = SparkSession.builder.appName("download-files")\
                        .config("spark.driver.memory", "4g")\
                        .getOrCreate()
        self.sc = self.spark.sparkContext

    def download_dirs(self, remote_dir, local_dir):
        #configs = self.spark.sparkContext.getConf()
        #print(configs.getAll())
        sc = self.spark.sparkContext
        ftp = FTP(self.ftp_host)
        ftp.login()
        ftp.cwd(remote_dir)
        current_dir = ftp.pwd()
        all_items = ftp.nlst()
        ftp.quit()

        print(all_items)
        all_remote_items = list(map(lambda x: os.path.join(current_dir, x), all_items))
        all_local_items = list(map(lambda x: os.path.join(local_dir, x), all_items))
        all_items = zip(all_remote_items, all_local_items)
        file_rdd = self.sc.parallelize(all_items)
        broadcast_var = self.sc.broadcast(self.ftp_host)

        def download_files_recursively(dirs):
            remote_dir, local_dir = dirs
            print(remote_dir)
            files_dir = defaultdict(list)
            lftp = FTP(broadcast_var.value)
            lftp.login()
            #print(remote_file_path)
            try:
                #print(item)
                print("Remote_dir ", remote_dir)
                try:
                    lftp.cwd(remote_dir)
                    items = lftp.nlst()
                    for item in items:
                        print("Item >>" , item)
                        files_dir.update(download_files_recursively((os.path.join(remote_dir, item), os.path.join(local_dir, item))))
                except error_perm as e:
                    print("EXCEPTIONSDFJSD:LKFJSDL:KFJSDL:KFJ:SLDFJSLD:", remote_dir)
                    def write_data(data):
                        files_dir[local_dir].append(data)

                    lftp.retrbinary(f"RETR {remote_dir}", write_data)

                lftp.cwd("..")
            except Exception as e:
                print(f"{e}")
            lftp.quit()
            #print(variable)
            return files_dir

        all_files_rdd = file_rdd.map(download_files_recursively)
        all_file_list = all_files_rdd.collect()
        for item in all_file_list:
            print(item.keys())
        #remote_qualified_path_list = list(map(lambda x: os.path.join(current_dir, x[0]), all_file_list))


class DownloadFromHost:
    def __init__(self, host=""):
        self.ftp_host = host
        self.ftp = FTP(self.ftp_host)
        self.downloaded_files = {}
        self.file_download_lock = threading.Lock()
        self.file_write_lock = threading.Lock()
        self.file_lookup = {}
        self.data_lock = threading.Lock()
        self.data_written = 0

    def __del__(self):
        self.ftp.quit()

    def get_download_size(self, remote_dir=''):
        total_size = 0
        self.ftp.cwd(remote_dir)
        response = []
        self.ftp.retrlines('LIST', response.append)
        for line in response:
            parts = line.split(maxsplit=8)
            if len(parts) >= 9:
                file_size = int(parts[4])  # The file size is typically the 5th column (index 4)
                file_type = parts[0]  # File type (directory or file) is in the first column
                filename = parts[-1]  # Filename is the last column

            if file_type.startswith('d'):  # If it's a directory
                total_size += self.get_download_size(os.path.join(remote_dir, filename))
            else:
                total_size += file_size
        self.ftp.cwd("..")
        return total_size

    def start_file_download(self, remote_file_path="", local_file_path="", pbar=""):
        with self.file_download_lock:
            if remote_file_path in self.downloaded_files:
                return
        try:
            ftp = FTP(self.ftp_host)
            ftp.login()

            with open(local_file_path, "wb") as local_file:
                total_size = ftp.size(remote_file_path)
                def write_data(data):
                    local_file.write(data)
                    pbar.update(len(data))
                    with self.data_lock:
                        self.data_written += len(data)

                ftp.retrbinary(f"RETR {remote_file_path}", write_data)
            
            with self.file_download_lock:
                self.downloaded_files[remote_file_path]=local_file_path

            ftp.quit()
        except Exception as e:
            print(f"failed to download {remote_file_path}: {e}")

    def download_dirs(self, remote_dir="", local_path="",pbar=None):
        
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        ftp = FTP(self.ftp_host)
        ftp.login()
        ftp.cwd(remote_dir)

        items = ftp.nlst()
        ftp.quit()

        for item in items:
            local_item_path = os.path.join(local_path, item)
            remote_item_path = os.path.join(remote_dir, item)
            self.file_lookup[remote_item_path] = local_item_path
            if self.is_directory(remote_item_path):
                self.download_dirs(remote_item_path, local_item_path, pbar)
            else:
                with self.file_write_lock:
                    self.start_file_download(remote_item_path, local_item_path, pbar)


    def is_directory(self, item):
        try:
            ftp = FTP(self.ftp_host)
            ftp.login()
            ftp.cwd(item)
            ftp.quit()
            return True
        except Exception:
            return False

    def download_dir(self, remote_resource, local_path):
        self.ftp.login()
        print("Calculating Payload Size")
        remote_file_size = 551763326 #precomputed value to save time
        #remote_file_size = self.get_download_size(remote_resource)
        with tqdm(total=remote_file_size, unit="B", unit_scale=True, desc="Progress") as pbar:
            self.download_dirs(remote_resource, local_path, pbar) 
            #print(self.file_lookup)
            #with self.file_write_lock, self.file_download_lock:
            remaining_set = set(self.file_lookup.items()) - set(self.downloaded_files.items())
            print(remaining_set)
            if remaining_set:
                for remote_file_path, local_file_path in remaining_set:
                    self.start_file_download(remote_file_path, local_file_path, pbar)

class ParseHeaders:
    def __init__(self):
        pass

    def parse(self, access_file):
        parser = AccessParser(access_file)
        for table in parser.catalog:
            if table != "tables":
                data = parser.parse_table(table)
            else:
                # strip all the extra spaces from STUBS
                data = parser.parse_table(table)
                stripped_stubs = []
                for val in data['STUB']:
                    if val:
                        stripped_stubs.append(val.strip())
                    else:
                        stripped_stubs.append(val)
                data['STUB'] = stripped_stubs

            with open(f'{LOCAL_PATH}/0File_Structure/{table}.csv', mode='w', newline='') as file:
                fieldnames = data.keys()
                writer = csv.DictWriter(file, fieldnames=fieldnames)

                writer.writeheader()

                for i in range(len(next(iter(data.values())))):
                    row = {key: data[key][i] for key in data}
                    writer.writerow(row)


# download required files.
download_obj = DownloadFromHostNew(HOST)
download_obj.download_dirs(REMOTE_RESOURCE, LOCAL_PATH)

# unzip using rui's script
batch_process(LOCAL_PATH, LOCAL_PATH)

# parse access database files
ParseHeaders().parse(ACCESS_FILE)


