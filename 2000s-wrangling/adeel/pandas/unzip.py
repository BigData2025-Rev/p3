import os
import zipfile

def mass_unzip(source_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                extract_path = os.path.join(output_dir, os.path.relpath(root, source_dir))

                os.makedirs(extract_path, exist_ok=True)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_path)
                print(f"Extracted: {zip_path} -> {extract_path}")

source_directory = r"C:\Users\adeel\Desktop\Project 3\redistricting_file--pl_94-171"
output_directory = r"C:\Users\adeel\Desktop\Project 3\unzipped_redistrict_files"
mass_unzip(source_directory, output_directory)