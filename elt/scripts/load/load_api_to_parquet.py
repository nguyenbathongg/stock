import os
import json
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

def get_latest_file_in_directory(directory, extension):
    """
    Get the latest file in a directory based on the last modified time.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory)]

    if not files:
        raise FileNotFoundError(f"No files found in directory: {directory}")

    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def load_json_from_file(file_path):
    """
    Load JSON data from a file.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def save_json_to_parquet(data, output_filepath):
    # Convert json data to pyarrow table
    table = pa.Table.from_pandas(pd.DataFrame(data))

    pq.write_table(table, output_filepath)

def load_db_to_dl(input_directory, output_directory):
    """
    Load JSON data from a directory and save it as Parquet files.
    """
    # Get the latest file in the input directory
    extension = '.json'
    latest_file = get_latest_file_in_directory(input_directory, extension)

    if latest_file:
        

        # Load JSON data from the latest file
        data = load_json_from_file(latest_file)
        print(f"Loaded data from {latest_file}")

        file_name = os.path.basename(latest_file).replace(extension, '.parquet')
        output_filepath = os.path.join(output_directory, file_name)


        # Save the data to a Parquet file
        save_json_to_parquet(data, output_filepath)
        print(f"Saved data to {output_filepath}")
    else:
        print(f"No JSON files found in {input_directory}")


# Convert News JSON files to Parquet
# Path to the directory containing the JSON files
input_directory = r'/home/thong/WorkSpace/Project/elt/data/raw/news'
# Path to the directory to save the Parquet files
output_directory = r'/home/thong/WorkSpace/Project/elt/data/completed/load_api_news_to_dl'
load_db_to_dl(input_directory, output_directory)

# Convert OHLCv JSON files to Parquet
# Path to the directory containing the JSON files
input_directory = r'/home/thong/WorkSpace/Project/elt/data/raw/ohlcv'
# Path to the directory to save the Parquet files
output_directory = r'/home/thong/WorkSpace/Project/elt/data/completed/load_api_ohlcv_to_dl'
load_db_to_dl(input_directory, output_directory)