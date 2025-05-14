import psycopg2
import json
import os

def get_latest_file_in_directory(directory, extension):
    """
    Get the latest file in a directory with a specific extension.
    
    :param directory: Directory to search for files.
    :param extension: File extension to look for.
    :return: Path to the latest file or None if no files are found.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def insert_data_from_json_to_db(filepath, table_name, columns, conflict_columns):
    """
    Insert data from a JSON file into a PostgreSQL database table.
    
    :param filepath: Path to the JSON file.
    :param table_name: Name of the database table.
    :param columns: List of columns in the table.
    :param conflict_columns: List of columns to check for conflicts.
    """
    # Read the JSON file
    with open(filepath, 'r') as file:
        data = [json.loads(line) for line in file]

    if not data:
        print(f"No data found in {filepath}")
        return
    
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)

    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_columns_str}) DO NOTHING
    """

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname='datasource',
        user='postgres',
        password='1',
        host='localhost',
        port='5432'
    )
    cursor = conn.cursor()

    for record in data:
        values = [record[col] for col in columns]
        cursor.execute(query, values)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data from {filepath} inserted into {table_name} table.")

if __name__ == "__main__":
    # Insert data into 'regions' table
    insert_data_from_json_to_db(
        get_latest_file_in_directory(
            '/home/thong/WorkSpace/Project/backend/data/processed/transformed_to_database_regions',
            '.json'
        ),
        'regions',
        ['region_name', 'region_local_open', 'region_local_close'],
        ['region_name']
    )

    # Insert data into 'industries' table
    insert_data_from_json_to_db(
        get_latest_file_in_directory(
            '/home/thong/WorkSpace/Project/backend/data/processed/transformed_to_database_industries',
            '.json'
        ),
        'industries',
        ['industry_name', 'industry_sector'],
        ['industry_name', 'industry_sector']
    )

    # Insert data into 'sicindustries' table
    insert_data_from_json_to_db(
        get_latest_file_in_directory(
            '/home/thong/WorkSpace/Project/backend/data/processed/transformed_to_database_sicindustries', 
            '.json'
        ),
        'sicindustries',
        ['sic_id', 'sic_industry', 'sic_sector'],
        ['sic_industry', 'sic_sector']
    )

