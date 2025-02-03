import os
import pandas as pd

def parse_upl_structure(structure_file):
    structure = pd.read_csv(structure_file)
    start = 0
    colspecs = []
    headers = []

    for _, row in structure.iterrows():
        length = int(float(row["LEN"])) 
        colspecs.append((start, start + length))
        headers.append(row["NAME"])
        start += length

    return colspecs, headers

def process_comma_separated(file_path, headers_file, output_csv):
    headers = pd.read_csv(headers_file).columns.tolist()
    df = pd.read_csv(file_path, header=None)
    df.columns = headers
    df.to_csv(output_csv, index=False)
    print(f"Converted {file_path} to {output_csv}")

def process_fixed_width(file_path, structure_file, output_csv):
    colspecs, headers = parse_upl_structure(structure_file)
    df = pd.read_fwf(file_path, colspecs=colspecs, header=None, encoding='latin1')
    df.columns = headers
    df.to_csv(output_csv, index=False)
    print(f"Converted {file_path} to {output_csv}")

def batch_convert_all(source_dir, output_dir, header_00001, header_00002, structure_file):
    os.makedirs(output_dir, exist_ok=True)

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".upl"):
                file_path = os.path.join(root, file)
                rel_dir = os.path.relpath(root, source_dir)
                output_subdir = os.path.join(output_dir, rel_dir)
                os.makedirs(output_subdir, exist_ok=True)

                if file.endswith("00001.upl"):
                    output_csv = os.path.join(output_subdir, f"{os.path.splitext(file)[0]}.csv")
                    process_comma_separated(file_path, header_00001, output_csv)

                elif file.endswith("00002.upl"):
                    output_csv = os.path.join(output_subdir, f"{os.path.splitext(file)[0]}.csv")
                    process_comma_separated(file_path, header_00002, output_csv)

                elif file.endswith("geo.upl"):
                    output_csv = os.path.join(output_subdir, f"{os.path.splitext(file)[0]}.csv")
                    process_fixed_width(file_path, structure_file, output_csv)


source_directory = r"C:\Users\adeel\Desktop\Project 3\unzipped_redistrict_files"
output_directory = r"C:\Users\adeel\Desktop\Project 3\csv"

structure_file_path = r"C:\Users\adeel\Desktop\Project 3\PL2000_Access2000.mdb\header.csv"
header_00001_path = r"C:\Users\adeel\Desktop\Project 3\PL2000_Access2000.mdb\PL_Part1.csv"
header_00002_path = r"C:\Users\adeel\Desktop\Project 3\PL2000_Access2000.mdb\PL_Part2.csv"

batch_convert_all(source_directory, output_directory, header_00001_path, header_00002_path, structure_file_path)
