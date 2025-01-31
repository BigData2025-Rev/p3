import os
import pandas as pd


def read_fixed_width_file(file_path, header_path):
    """
    按固定宽度读取文件。
    :param file_path: 要读取的文件路径（geo.upl）。
    :param header_path: 表头描述文件路径（header.csv）。
    :return: 包含数据的 DataFrame。
    """
    header_df = pd.read_csv(header_path)
    col_names = header_df["NAME"].tolist()
    col_widths = header_df["LEN"].tolist()
    df = pd.read_fwf(file_path, widths=col_widths, names=col_names)
    return df


def process_files(state_dir, output_dir, geo_header_path, file01_header_path, file02_header_path):
    """
    处理每个州的地理文件、数据文件1和数据文件2。
    """
    # 搜索文件
    files = os.listdir(state_dir)
    geo_file = next((f for f in files if "geo" in f.lower()), None)
    file_01 = next((f for f in files if "00001" in f.lower()), None)
    file_02 = next((f for f in files if "00002" in f.lower()), None)

    if not geo_file or not file_01 or not file_02:
        print(f"Missing files in {state_dir}")
        return

    # 构造完整文件路径
    geo_file_path = os.path.join(state_dir, geo_file)
    file_01_path = os.path.join(state_dir, file_01)
    file_02_path = os.path.join(state_dir, file_02)

    # 从 header 文件读取表头
    file01_headers = pd.read_csv(file01_header_path).columns.tolist()
    file02_headers = pd.read_csv(file02_header_path).columns.tolist()

    # 读取数据文件
    geo_df = read_fixed_width_file(geo_file_path, geo_header_path)
    file01_df = pd.read_csv(file_01_path, names=file01_headers, skiprows=1)
    file02_df = pd.read_csv(file_02_path, names=file02_headers, skiprows=1)

    # 确保输出目录存在
    state_output_dir = os.path.join(output_dir, os.path.basename(state_dir))
    os.makedirs(state_output_dir, exist_ok=True)

    # 保存单独的 CSV 文件
    geo_output = os.path.join(state_output_dir, "geo.csv")
    file01_output = os.path.join(state_output_dir, "00001.csv")
    file02_output = os.path.join(state_output_dir, "00002.csv")

    geo_df.to_csv(geo_output, index=False)
    file01_df.to_csv(file01_output, index=False)
    file02_df.to_csv(file02_output, index=False)

    print(f"Saved: {geo_output}")
    print(f"Saved: {file01_output}")
    print(f"Saved: {file02_output}")


# 主程序
if __name__ == "__main__":
    base_dir = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171/"  # 包含各州文件夹的目录
    output_dir = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/output/"  # 输出目录

    geo_header_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171/0File_Structure/header.csv"
    file01_header_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171/0File_Structure/PL_Part1.csv"
    file02_header_path = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171/0File_Structure/PL_Part2.csv"

    os.makedirs(output_dir, exist_ok=True)

    for state in os.listdir(base_dir):
        state_dir = os.path.join(base_dir, state)
        if os.path.isdir(state_dir):
            process_files(state_dir, output_dir, geo_header_path, file01_header_path, file02_header_path)
