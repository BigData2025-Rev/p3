import os
import pandas as pd


def read_fixed_width_file(file_path, header_path):
    # geo file is spereted with fixed length
    # column name: from the header.csv:NAME column
    # column width: from the header.csv:LEN column
    header_df = pd.read_csv(header_path)
    return pd.read_fwf(file_path, widths=header_df["LEN"].tolist(), names=header_df["NAME"].tolist())


def process_files(state_dir, output_dir, geo_header_path, file01_header_path, file02_header_path):
    # Processes geo, 00001, and 00002 files in a state's directory
    files = {f.lower(): f for f in os.listdir(state_dir)}
    
    geo_file = files.get(next((f for f in files if "geo" in f), None))
    file_01 = files.get(next((f for f in files if "00001" in f), None))
    file_02 = files.get(next((f for f in files if "00002" in f), None))

    if not all([geo_file, file_01, file_02]):
        print(f"Missing files in {state_dir}")
        return

    # Read files and transfer to dataframe
    geo_df = read_fixed_width_file(os.path.join(state_dir, geo_file), geo_header_path)
    file01_df = pd.read_csv(os.path.join(state_dir, file_01), names=pd.read_csv(file01_header_path).columns.tolist(), skiprows=1)
    file02_df = pd.read_csv(os.path.join(state_dir, file_02), names=pd.read_csv(file02_header_path).columns.tolist(), skiprows=1)

    # Ensure output directory exists
    state_output_dir = os.path.join(output_dir, os.path.basename(state_dir))
    os.makedirs(state_output_dir, exist_ok=True)

    # Save dataframe
    for df, name in [(geo_df, "geo.csv"), (file01_df, "00001.csv"), (file02_df, "00002.csv")]:
        output_path = os.path.join(state_output_dir, name)
        df.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")


if __name__ == "__main__":
    BASE_DIR = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171"
    OUTPUT_DIR = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/output"
    HEADER_DIR = os.path.join(BASE_DIR, "0File_Structure")

    geo_header_path = os.path.join(HEADER_DIR, "header.csv")
    file01_header_path = os.path.join(HEADER_DIR, "PL_Part1.csv")
    file02_header_path = os.path.join(HEADER_DIR, "PL_Part2.csv")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for state in os.listdir(BASE_DIR):
        state_dir = os.path.join(BASE_DIR, state)
        if os.path.isdir(state_dir):
            process_files(state_dir, OUTPUT_DIR, geo_header_path, file01_header_path, file02_header_path)
