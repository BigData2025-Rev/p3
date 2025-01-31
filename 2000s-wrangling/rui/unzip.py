import os
import zipfile


def unzip_file(zip_path, output_folder):
    """Extracts a ZIP file, ensuring extracted files overwrite existing ones."""
    os.makedirs(output_folder, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(output_folder)
        for file_name in zip_ref.namelist():
            print(f"Extracted: {file_name}")


def convert_upl_files(directory):
    """Converts all .upl files in subdirectories from LF to CRLF line endings as documentation required."""
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".upl"):
                file_path = os.path.join(root, file)

                # Read file and check if conversion is needed
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                if '\r\n' not in content:  # Convert only if necessary
                    with open(file_path, 'w', encoding='utf-8', errors='ignore') as f:
                        f.write(content.replace('\n', '\r\n'))
                    print(f"Converted: {file_path}")


def batch_process(input_dir, output_dir):
    """Processes all ZIP files in subdirectories and converts .upl files."""
    for state_folder in os.listdir(input_dir):
        state_path = os.path.join(input_dir, state_folder)

        if os.path.isdir(state_path):  # Ensure it's a directory
            for file in os.listdir(state_path):
                if file.endswith(".zip"):
                    zip_path = os.path.join(state_path, file)
                    state_output_folder = os.path.join(output_dir, state_folder)
                    print(f"Unzipping: {zip_path} -> {state_output_folder}")
                    unzip_file(zip_path, state_output_folder)

    print("Converting .upl files...")
    convert_upl_files(output_dir)


# Define paths
BASE_DIR = "/mnt/c/Personal_Official/Project/revature/project3/data/2000/redistricting_file--pl_94-171"
OUTPUT_DIR = BASE_DIR  # Same as input directory

batch_process(BASE_DIR, OUTPUT_DIR)
