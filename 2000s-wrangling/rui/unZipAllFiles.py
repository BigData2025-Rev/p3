import os
import zipfile

def unzip_file(zip_file, output_folder):
    """
    解压单个 ZIP 文件，并确保解压内容覆盖已有文件。
    """
    os.makedirs(output_folder, exist_ok=True)  # 创建输出目录（如不存在）
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            extracted_path = os.path.join(output_folder, file_name)
            # 强制覆盖解压文件
            zip_ref.extract(file_name, output_folder)
            print(f"Extracted {file_name} to {extracted_path}")

def convert_upl_files(output_folder):
    """
    遍历 output_folder 中的所有子文件夹，转换 .upl 文件的 LF 为 CRLF。
    """
    # 遍历 output_folder 的所有子文件夹
    for state_folder in os.listdir(output_folder):
        state_path = os.path.join(output_folder, state_folder)

        if os.path.isdir(state_path):  # 确保是州文件夹
            print(f"Processing upl files in state folder: {state_folder}")

            # 遍历州文件夹中的 upl 文件
            for root, _, files in os.walk(state_path):
                for file in files:
                    if file.endswith(".upl"):
                        file_path = os.path.join(root, file)

                        # 检查文件内容是否需要转换（避免重复转换）
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()

                        # 如果文件中已包含 CRLF，则跳过
                        if '\r\n' in content:
                            continue

                        # 转换并写回文件
                        with open(file_path, 'w', encoding='utf-8', errors='ignore') as f:
                            f.write(content.replace('\n', '\r\n'))

                        print(f"Converted LF to CRLF in {file_path}")

def batch_process(base_dir, output_dir):
    """
    批量处理：先解压所有 ZIP 文件，然后统一转换 upl 文件换行符。
    """
    # 解压所有 ZIP 文件
    for state_folder in os.listdir(base_dir):
        state_path = os.path.join(base_dir, state_folder)

        if os.path.isdir(state_path):  # 确保是州文件夹
            print(f"Processing state folder: {state_folder}")

            for file in os.listdir(state_path):
                if file.endswith(".zip"):
                    zip_file = os.path.join(state_path, file)
                    state_output_folder = os.path.join(output_dir, state_folder)
                    print(f"Unzipping {zip_file} to {state_output_folder}...")
                    unzip_file(zip_file, state_output_folder)

    # 全局转换 upl 文件
    print("Starting conversion of upl files...")
    convert_upl_files(output_dir)

# 设置文件路径

base_dir = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171"
output_dir = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171"

batch_process(base_dir, output_dir)