import os
import pandas as pd

def combine_state_files(file_00001, file_00002, file_geo, output_file):
    df_00001 = pd.read_csv(file_00001)
    df_00002 = pd.read_csv(file_00002)
    df_geo = pd.read_csv(file_geo)

    cols_to_drop = ["FILEID", "STUSAB", "CHARITER", "CIFSN"]
    df_00002 = df_00002.drop(columns=cols_to_drop)
    df_geo = df_geo.drop(columns=cols_to_drop)

    # Join the dataframes on LOGRECNO
    combined_df = (
        df_00001
        .merge(df_00002, on="LOGRECNO", how="left")
        .merge(df_geo, on="LOGRECNO", how="left")
    )

    # drop logrecno after merge (only from 00002 and geo file)
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]

    combined_df.to_csv(output_file, index=False)
    print(f"Combined file saved to: {output_file}")

def batch_combine_all_states(input_dir, output_dir):
    for state_folder in os.listdir(input_dir):
        state_path = os.path.join(input_dir, state_folder)


        if os.path.isdir(state_path):
            file_00001 = None
            file_00002 = None
            file_geo = None

            for file in os.listdir(state_path):
                if file.endswith("00001.csv"):
                    file_00001 = os.path.join(state_path, file)
                elif file.endswith("00002.csv"):
                    file_00002 = os.path.join(state_path, file)
                elif file.endswith("geo.csv"):
                    file_geo = os.path.join(state_path, file)


            if file_00001 and file_00002 and file_geo:
                output_state_dir = os.path.join(output_dir, state_folder)
                os.makedirs(output_state_dir, exist_ok=True)
                output_file = os.path.join(output_state_dir, f"{state_folder}_combined.csv")
                combine_state_files(file_00001, file_00002, file_geo, output_file)
            else:
                print(f"Missing files for state folder: {state_folder}")

# Input and output directories
input_directory = r"C:\Users\adeel\Desktop\Project 3\csv"
output_directory = r"C:\Users\adeel\Desktop\Project 3\combined_csv"

batch_combine_all_states(input_directory, output_directory)