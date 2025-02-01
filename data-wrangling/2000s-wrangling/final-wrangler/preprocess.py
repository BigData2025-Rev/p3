
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ftplib import FTP
import csv

from access_parser import AccessParser
from tqdm import tqdm

from rui.unZipAllFiles import batch_process


REMOTE_RESOURCE = "/census_2000/datasets/redistricting_file--pl_94-171"
LOCAL_PATH = "../../../TestDownloadData"
ACCESS_FILE = os.path.join(LOCAL_PATH, "0File_Structure/Access2000/PL2000_Access2000.mdb")


class DownloadFromHost:
    def __init__(self, host=""):
        self.ftp_host = host
        self.ftp = FTP(self.ftp_host)

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

    def start_download(self, remote_dir="", local_path="", pbar=''):
        self.ftp.cwd(remote_dir)

        items = self.ftp.nlst()
        for item in items:
            local_item = os.path.join(local_path, item)
            try:
                self.ftp.cwd(item)
                is_dir = True
                self.ftp.cwd('..')
            except Exception as e:
                is_dir = False

            if is_dir:
                if not os.path.exists(local_item):
                    os.makedirs(local_item)
                self.start_download(item, local_item, pbar)
            else:
                try:
                    remote_file_size = self.ftp.size(item)
                except Exception as e:
                    remote_file_size = 0

                with open(local_item, "wb") as local:
                    def write_data(data):
                        local.write(data)
                        pbar.update(len(data))

                    self.ftp.retrbinary(f"RETR {item}", write_data)

            #print(f"Downloaded {item} to {local_item}")
        self.ftp.cwd("..")

    def download_dir(self, remote_resource, local_path):
        self.ftp.login()
        print("Calculating Payload Size")
        remote_file_size = 551763326 #precomputed value to save time
        #remote_file_size = self.get_download_size(remote_resource)
        with tqdm(total=remote_file_size, unit="B", unit_scale=True, desc="Progress") as pbar:
            self.start_download(remote_resource, local_path, pbar)


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
download_obj = DownloadFromHost("ftp.census.gov")
download_obj.download_dir(REMOTE_RESOURCE, LOCAL_PATH)

# unzip using rui's script
batch_process(LOCAL_PATH, LOCAL_PATH)

# parse access database files
ParseHeaders().parse(ACCESS_PATH)


